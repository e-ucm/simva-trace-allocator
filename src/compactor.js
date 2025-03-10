import { now, duration, formatDuration } from './utils/date.js';
import { logger } from './logger.js';
import { MinioClient } from './minio.js'; 
import { SimvaClient } from './simva.js';
import { getState } from './state.js';
import { binarySearch, diffArray } from './utils/misc.js';
import { sha1sums } from './utils/sha.js';

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
            const remotePath = activityState.remoteOutputPath;
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

        let activitiesToGo = await this.#garbageCollectActivities(state, activities);

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
            logger.info('Activities to removed OK.');
        }
        logger.info('Starting collecting state garbage.');
        // Garbage collect state files in activities
        const activityToPass = await state.garbageCollect();
        logger.info('Collecting state garbage finished.');
        return activityToPass;
    }

    /**
     * 
     * @param {ActivityCompactionState} activityState 
     * @returns {Promise<boolean>} false if nothing new
     */
    async #updateActivityTraces(activityState) {
        let traceFiles = (await this.#minio.getTraces(activityState.activityId)).map((o) => o.name);
        traceFiles.sort();
        const sha1 = sha1sums(traceFiles);
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
        const remoteStatePath = activityState.remoteStatePath;
        const remotePath = activityState.remoteOutputPath;
        try {
            const metadata = {
                "Content-Type": "application/json",
                "Version": "1"
            };
            await this.#minio.copyToRemoteFile(localStatePath, remotePath, metadata);
            //await this.#minio.copyWithinMinIO(remoteStatePath, remotePath);
            logger.info("Object copied successfully!");
        } catch (error) {
            logger.error("Copy failed:");
            logger.error(error);
        }
        logger.info(`Copied compacted file for activity %s`, activityState.activityId);
    }

    async processConsistencyAndGarbage() {
        logger.info('Check consistency');
        await this.#checkConsistency();
        logger.info('Start compaction');
        await this.#compactActivities();
    }

    // Method to process messages (acts as the callback for KafkaClient)
    /**
     * @param {any} message
     */
    async processMessage(message) {
        // Log the received message
        logger.debug('Received message:');
        logger.debug(message.value);

        let state = await getState(this.#opts, this.#minio);
        // Set up the delimiter and the required bucket and path values
        let delimiter = '/';
        let bucket = this.#opts.minio.bucket;
        
        // Build the path to traces topic
        let tracestopicspath = `${this.#opts.minio.topics_dir}${delimiter}${this.#opts.minio.traces_topic}${delimiter}_id=`;
    
        // Log the constructed path
        logger.debug(`Trace topic path: ${tracestopicspath}`);
    
        // Parse the message value (assuming it's a JSON string)
        let ev = JSON.parse(message.value);
        let key = ev.Key;
        
        // Log the key extracted from the message
        logger.debug(`Received Key: ${key}`);
    
        // Remove the bucket and trace topic path from the key to get the key value
        let keyvalue = key.replace(`${bucket}${delimiter}${tracestopicspath}`, "");
        
        // Log the key value after removal
        logger.debug(`Key value without bucket and path: ${keyvalue}`);
    
        // Split the key value to extract activityId and filename
        let added = keyvalue.split(delimiter);
        
        // Initialize variables for activityId and filename
        let activityId = null;
        let filename = null;
        let keyWithoutBucket = null;
    
        // If the split key has exactly 2 parts, extract activityId and filename
        if (added.length === 2) {
            throw new Error('Key format is unexpected. Unable to extract activityId and filename.');
        }
        activityId = added[0];
        filename = added[1];
        keyWithoutBucket = `${tracestopicspath}${activityId}${delimiter}${filename}`;

        // Log the extracted values
        logger.debug(`activityId: ${activityId}, filename: ${filename}, key: ${key}, keyWithoutBucket: ${keyWithoutBucket}`);
        
        // ActivityState
        let activityState = state.get(activityId);
        if (activityState === undefined) {
            logger.debug(`New activity: %s`, activityId);
            activityState = await state.create(activityId);
        }
        logger.debug(activityState);
        // compute which files need to be appended
        const activityFiles = (await activityState.files());
        logger.debug(activityFiles);
        
        if(activityFiles.includes(keyWithoutBucket)) {
            logger.warn("Already consumed: %s", keyWithoutBucket);
            return;
        }

        let positionvalue=-binarySearch(activityFiles, keyWithoutBucket, true, (a,b)=> { 
            if(typeof a == "string" && typeof b == "string" ) {
                return a.localeCompare(b); 
            } else {
                return -1;
            }
        })-1;

        const nextposition = activityFiles.length;

        logger.debug(keyWithoutBucket);
        logger.debug("positionvalue:");
        logger.debug(positionvalue);
        logger.debug("nextposition:");
        logger.debug(nextposition);

        if(positionvalue < nextposition) {
            logger.warn("Not ordered. Should have been consumed before.")
        }

        activityFiles.splice(nextposition, 0, keyWithoutBucket);
        logger.debug(activityFiles);
        const sha1 = sha1sums(activityFiles);
        await this.#updateActivityTracesFromPath(activityState, keyWithoutBucket, sha1);
        logger.debug(activityState);

        await this.#distributeTrace(activityState);
        logger.debug(activityState);

        await state.save();
    }
     
    /**
     * Update Activity Traces From Path
     * @param {ActivityCompactionState} activityState 
     * @param {string} keyPath
     * @param {string} sha1
     * @returns {Promise<boolean>} false if nothing new
     */
    async #updateActivityTracesFromPath(activityState, keyPath, sha1) {
        const nowDate = now();
        const filesToAdd = [keyPath];
        logger.info(`Compacting activity %s`, activityState.activityId);
        logger.debug(filesToAdd);
        await activityState.update(filesToAdd, nowDate, sha1);
        return true;
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
