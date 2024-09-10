import { now, duration, formatDuration } from './utils/date.js';
import { logger } from './logger.js';
import { MinioClient } from './minio.js';
import { KafkaClient } from './kafka.js';
import { getState } from './state.js';
import { createHash } from 'node:crypto';

/** @typedef {import('./config.js').CompactorOptions} CompactorOptions */
/** @typedef {import('./state.js').ActivityCompactionState} ActivityCompactionState */
/** @typedef {import('./state.js').CompactorState} CompactorState */

/**
 * @typedef CompactorStatus
 * @property {boolean} processing
 * @property {number} current
 * @property {number} total
 * @property {Date} [startTime]
 */

export class KafkaCompactor {
    /**
     * @param {CompactorOptions} opts
     */
    constructor(opts) {
        this.#opts = opts;
        this.#minio = new MinioClient(opts.minio);
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

    /** @type {boolean} */
    shouldExit;

    /** @type {CompactorStatus} */
    status;

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
                await this.#updateActivityTraces(activityState, keyWithoutBucket);
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

    // Method to start consuming messages using KafkaClient
    async start() {
        try {
            logger.info('Compactor starting Kafka consumption...');
            // Start Kafka consumption and pass the processMessage as a callback
            await this.#kafka.consumeLatestMessages(this.processMessage.bind(this));
        } catch (error) {
            console.error('Error starting Compactor:', error);
        }
    }

    // Method to stop consuming messages
    async stop() {
        try {
            await this.#kafka.disconnect();
            logger.info('Compactor stopped Kafka consumption.');
        } catch (error) {
            console.error('Error stopping Compactor:', error);
        }
    }

    /**
     * Update Activity Traces
     * @param {ActivityCompactionState} activityState 
     * @param {string} keyPath
     * @returns {Promise<boolean>} false if nothing new
     */
    async #updateActivityTraces(activityState, keyPath) {
        const hash = this.createSha1();
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
    
    /**
     * Distribute trace 
     * @param {ActivityCompactionState} activityState 
     */
    async #distributeTrace(activityState) {
        const localStatePath = activityState.localStatePath;
        const outputDir = this.#opts.minio.outputs_dir;
        const tracesFilename = this.#opts.minio.traces_file;
        const remotePath = `${outputDir}/${activityState.activityId}/${tracesFilename}`;
        logger.debug("remotePath:");
        logger.debug(remotePath);
        await this.#minio.copyToRemoteFile(localStatePath, remotePath);
        logger.info(`Copied compacted file for activity %s`, activityState.activityId);
    }

    /**
    * Create sha1 hash function.
    *
    * @returns 
    */
    createSha1() {
        return createHash('sha1');
    }


    /**
    * Get MinioClient 
    * @returns {MinioClient} client
    */
    getMinioClient() {
        return this.#minio;
    }

    /**
    * Get MinioClient 
    * @returns {CompactorOptions} options
    */
    getOpts() {
        return this.#opts;
    }
}