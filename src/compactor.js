import { now, duration, formatDuration } from './utils/date.js';
import { logger } from './logger.js';
import { MinioClient } from './minio.js'; 
import { SimvaClient } from './simva.js';
import { KafkaClient } from './kafka.js';
import { getState } from './state.js';
import { createHash } from 'node:crypto';
import { diffArray } from './utils/misc.js';

/** @typedef {import('./config.js').CompactorOptions} CompactorOptions */
/** @typedef {import('./simva.js').Activity} Activity */
/** @typedef {import('./state.js').ActivityCompactionState} ActivityCompactionState */
/** @typedef {import('./state.js').CompactorState} CompactorState */

/**
 * @typedef CompactorStatus
 * @property {boolean} processing
 * @property {number} current
 * @property {number} total
 * @property {Date} [startTime]
 */

export class Compactor {
    /**
     * @param {CompactorOptions} opts
     */
    constructor(opts) {
        this.#opts = opts;
        this.#minio = new MinioClient(opts.minio);
        this.#simva = new SimvaClient(opts.simva);
        this.#kafka = new KafkaClient(opts.kafka);
        this.shouldExit = false;
        this.status = {
            processing: false,
            current: -1,
            total: -1
        };
    }

    /** @type {CompactorOptions} */
    #opts;

    /** @type {MinioClient} */
    #minio;

    /** @type {KafkaClient} */
    #kafka;

    /** @type {SimvaClient} */
    #simva;

    /** @type {boolean} */
    shouldExit;

    /** @type {CompactorStatus} */
    status;

    async compact() {
        if (!this.status.processing) {
            this.status.processing = true;
            try {
                this.status.startTime = now();
                logger.info('Check consistency');
                await this.#checkConsistency();
                logger.info('Start compaction');
                await this.#compactActivities();
                const end = now();
                const durationstr = formatDuration(duration(this.status.startTime, end));
                logger.info('End of compaction, took: %s', durationstr);
            } catch (e) {
                logger.error(e);
                logger.error('####### ERROR WHILE PROCESSING THE TRACES. !!!!!!');
            }
            this.status.processing = false;
        } else {
            logger.info(`Compaction still running: ${this.elapsedTime}`);
        }
    }

    /**
     * @returns {string}
     */
    get elapsedTime() {
        if (!this.status.processing) {
            return 'Not running compactor';
        }
        const nowDate = now();
        const durationStr = formatDuration(duration(this.status.startTime, nowDate));
        return durationStr;
    }
    
    async #checkConsistency() {
        let state = await getState(this.#opts, this.#minio);

        let activities = await this.#simva.getActivities({ type: ['gameplay', 'miniokafka', 'rageminio'] });

        logger.info(`Known %d activities, received %d`, state.size, activities.length);

        const outputDir = this.#opts.minio.outputs_dir;
        const tracesFilename = this.#opts.minio.traces_file;

        this.status.total = activities.length;
        const inconsistent = [];
        for(let idx=0; idx < activities.length; idx++) {
            if (this.shouldExit) {
                break;
            }

            this.status.current = idx;
            const activity = activities[idx];
            logger.debug('Check consistency of activity: %s', activity._id);

            let activityState = state.get(activity._id);
            if (activityState === undefined) {
                logger.debug(`New activity, nothing to do: %s`, activity._id);
                continue;
            }

            let consistent = await activityState.checkConsistency();
            const remotePath = `${outputDir}/${activityState.activityId}/${tracesFilename}`;
                if (! await this.#minio.fileExists(remotePath) ) {
                    logger.warn('Compact file for activity \'%s\' not found: %s', activityState.activityId, remotePath);
                    consistent = false;
                }
            if (!consistent) {
                inconsistent.push(activity._id);
            }
        }
        if (!this.#opts.tryRecovery || inconsistent.length === 0) {
            return;
        }
        logger.info('Start recovery');
        for(const activity of inconsistent) {
            await state.remove(activity);
        }
        await state.save();
    }

    async #compactActivities() {
        let state = await getState(this.#opts, this.#minio);

        let activities = await this.#simva.getActivities({ type: ['gameplay', 'miniokafka', 'rageminio'] });

        logger.info(`Known %d activities, received %d`, state.size, activities.length);

        await this.#garbageCollectActivities(state, activities);

        this.status.total = activities.length;
        for(let idx=0; idx < activities.length; idx++) {
            if (this.shouldExit) {
                break;
            }

            this.status.current = idx;
            const activity = activities[idx];
            logger.debug('Processing activity: %s', activity._id);

            let activityState = state.get(activity._id);
            if (activityState === undefined) {
                logger.info(`New activity: %s`, activity._id);
                activityState = await state.create(activity._id);
            }

            const updated = await this.#updateActivityTraces(activityState);
            if (!updated) continue;
            await this.#distributeTrace(activityState);

            if (activities.length % 5) {
                await state.save();
            }
        }
        await state.save();
    }

    /**
     * @param {CompactorState} state
     * @param {Activity[]} activities
     */
    async #garbageCollectActivities(state, activities) {
        // Delete removed activites
        if (state.size > activities.length) {
            const removedActivities = [];
            const activitiesIds = new Set(activities.map(a => a._id));

            for(const knowActivityId of state.knownActivities) {
                if (! activitiesIds.has(knowActivityId)) {
                    removedActivities.push(knowActivityId);
                }
            }
            logger.debug('Activities to remove: %d', removedActivities.length);
            for(const activityId of removedActivities) {
                const activityState = state.get(activityId);
                if (!activityState) {
                    logger.warn('Activity to remove not found in global state !: %s', activityId);
                    continue;
                }
                try {
                    await state.remove(activityId);
                    logger.info('Activity removed: %s', activityId);
                } catch(error) {
                    logger.error('Could not remove activity: %s', activityId);
                    logger.error(error);
                }
            }
        }

        // Garbage collect state files in activities
        await state.garbageCollect();
    }

    /**
     * 
     * @param {ActivityCompactionState} activityState 
     * @returns {Promise<boolean>} false if nothing new
     */
    async #updateActivityTraces(activityState) {
        let traceFiles = (await this.#minio.getTraces(activityState.activityId)).map((o) => o.name);
        traceFiles.sort();
        const hash = createSha1();
        for(const traceFile of traceFiles) {
            hash.update(traceFile);
            hash.update('\n');
        }
        const sha1 = hash.digest('hex');
        if (sha1 === activityState.currentSha1) {
            logger.debug(`Nothing to do for activity %s`, activityState.activityId);
            return false;
        }

        // compute which files need to be appended
        const activityFiles = await activityState.files();
        const { added: filesToAdd } = diffArray(activityFiles, traceFiles);
        const nowDate = now();
        const elapsedTime = duration(activityState.lastUpdate, nowDate);
        if (filesToAdd.length < this.#opts.batchSize && elapsedTime < this.#opts.maxDelay) {
            const durationStr = formatDuration(elapsedTime);
            logger.debug(`Update postponed elapsedTime=%s, batchSize=%d for activity %s`, durationStr, filesToAdd.length, activityState.activityId);
            return false;
        }

        logger.info(`Compacting activity %s`, activityState.activityId);
        await activityState.update(filesToAdd, nowDate, sha1);
        return true;
    }

    /**
     * Distribute trace 
     * @param {ActivityCompactionState} activityState 
     */
    async #distributeTrace(activityState) {
        const localStatePath = activityState.localStatePath;
        const outputDir = this.#opts.minio.outputs_dir;
        const tracesFilename = this.#opts.minio.traces_file;
        const remotePath = `${outputDir}/${activityState.activityId}/${tracesFilename}`;
        await this.#minio.copyToRemoteFile(localStatePath, remotePath);
        logger.info(`Copied compacted file for activity %s`, activityState.activityId);
    }

    // Method to process messages (acts as the callback for KafkaClient)
    /**
     * @param {any} message
     */
    async processMessage(message) {
        try {
            // Log the received message
            logger.info('Received message:');
            logger.info(message.value);

            let state = await getState(this.#opts, this.#minio);
            
            // Set up the delimiter and the required bucket and path values
            let delimiter = '/';
            let bucket = this.#opts.minio.bucket;
            
            // Build the path to traces topic
            let tracestopicspath = `${this.#opts.minio.topics_dir}${delimiter}${this.#opts.minio.traces_topic}${delimiter}_id=`;
        
            // Log the constructed path
            logger.info(`Trace topic path: ${tracestopicspath}`);
        
            // Parse the message value (assuming it's a JSON string)
            let ev = JSON.parse(message.value);
            let key = ev.Key;
            
            // Log the key extracted from the message
            logger.info(`Received Key: ${key}`);
        
            // Remove the bucket and trace topic path from the key to get the key value
            let keyvalue = key.replace(`${bucket}${delimiter}${tracestopicspath}`, "");
            
            // Log the key value after removal
            logger.info(`Key value without bucket and path: ${keyvalue}`);
        
            // Split the key value to extract activityId and filename
            let added = keyvalue.split(delimiter);
            
            // Initialize variables for activityId and filename
            let activityId = null;
            let filename = null;
            let keyWithoutBucket = null;
        
            // If the split key has exactly 2 parts, extract activityId and filename
            if (added.length === 2) {
                activityId = added[0];
                filename = added[1];
                keyWithoutBucket = `${tracestopicspath}${activityId}${delimiter}${filename}`;

                // Log the extracted values
                logger.info(`activityId: ${activityId}, filename: ${filename}, key: ${key}, keyWithoutBucket: ${keyWithoutBucket}`);
                
                // ActivityState
                let activityState = state.get(activityId);
                if (activityState === undefined) {
                    logger.info(`New activity: %s`, activityId);
                    activityState = await state.create(activityId);
                }
                logger.info(activityState);
                await this.#updateActivityTracesFromPath(activityState, keyWithoutBucket);
                logger.info(activityState);
                await this.#distributeTrace(activityState);
                logger.info(activityState);
                await state.save();
            } else {
                logger.warn('Key format is unexpected. Unable to extract activityId and filename.');
            }
        } catch(e) {
            logger.debug('Error processing message:');
            logger.debug(e);
        }
    }
     
    /**
     * Update Activity Traces From Path
     * @param {ActivityCompactionState} activityState 
     * @param {string} keyPath
     * @returns {Promise<boolean>} false if nothing new
     */
    async #updateActivityTracesFromPath(activityState, keyPath) {
        const hash = createSha1();
        hash.update(keyPath);
        hash.update('\n');
        const sha1 = hash.digest('hex');
        const nowDate = now();
        const filesToAdd = [keyPath];
        logger.info(`Compacting activity %s`, activityState.activityId);
        logger.info(filesToAdd);
        await activityState.update(filesToAdd, nowDate, sha1);
        return true;
    }

    // Method to start consuming messages using KafkaClient
    async startKafkaConsumer() {
        try {
            logger.info('Compactor starting Kafka consumption...');
            // Start Kafka consumption and pass the processMessage as a callback
            await this.#kafka.consumeLatestMessages(this.processMessage.bind(this));
        } catch (error) {
            console.error('Error starting Compactor:', error);
        }
    }

    // Method to stop consuming messages
    async stopKafkaConsumer() {
        try {
            await this.#kafka.disconnect();
            logger.info('Compactor stopped Kafka consumption.');
        } catch (error) {
            console.error('Error stopping Compactor:', error);
        }
    }
    
    /**
    * Get MinioClient 
    * @returns {MinioClient} client
    */
    getMinioClient() {
        return this.#minio;
    }
    
    /**
    * Get CompactorOptions 
    * @returns {CompactorOptions} options
    */
    getOpts() {
        return this.#opts;
    }
}

/**
 * Create sha1 hash function.
 *
 * @returns 
 */
function createSha1() {
	return createHash('sha1');
}
