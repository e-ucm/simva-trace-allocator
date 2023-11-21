import { now, duration, formatDuration } from './dateUtils.js';
import { logger } from './logger.js';
import { MinioClient } from './minio.js';
import { SimvaClient } from './simva.js';
import { getState } from './state.js';
import { createHash } from 'node:crypto';
import { diffArray } from './utils.js';

/** @typedef {import('./config.js').CompactorOptions} CompactorOptions */
/** @typedef {import('./simva.js').Activity} Activity */
/** @typedef {import('./state.js').ActivityCompactionState} ActivityCompactionState */

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

    async #compactActivities() {
        let state = await getState(this.#opts, this.#minio);

        let activities = await this.#simva.getActivities({ type: ['gameplay', 'miniokafka', 'rageminio'] });

        logger.info(`Known %d activities, received %d`, state.size, activities.length);

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

            await this.#updateOwners(activity, activityState);
            const updated = await this.#updateActivityTraces(activity, activityState);
            if (!updated) continue;
            await this.#distributeTraceToOwners(activity, activityState);

            if (activities.length % 5) {
                await state.save();
            }
        }
        await state.save();
    }

    /**
     * 
     * @param {Activity} activity 
     * @param {ActivityCompactionState} activityState 
     */
    async #updateOwners(activity, activityState) {
        const diffOwners = diffArray(activityState.owners, activity.owners.sort());
        // Remove files
        if (diffOwners.removed.length > 0) {
            logger.info('Removing owners [%s] for activity: %s', diffOwners.removed.join(', '), activity._id);
            for(const removedOwner of diffOwners.removed) {
                await this.#minio.removeCompactedFileForUser(activity._id, removedOwner);
            }
        }
        // Update State
        const updatedOwners = [];
        for(const owner of activityState.owners) {
            if (diffOwners.removed.indexOf(owner) === -1) {
                updatedOwners.push(owner);
            }
        }

        activityState.owners = updatedOwners.concat(diffOwners.added);
    }

    /**
     * 
     * @param {Activity} activity 
     * @param {ActivityCompactionState} activityState 
     * @returns {Promise<boolean>} false if nothing new
     */
    async #updateActivityTraces(activity, activityState) {
        let traceFiles = (await this.#minio.getTraces(activity._id)).map((o) => o.name);
        traceFiles.sort();
        const hash = createSha1();
        for(const traceFile of traceFiles) {
            hash.update(traceFile);
            hash.update('\n');
        }
        const sha1 = hash.digest('hex');
        if (sha1 === activityState.currentSha1) {
            logger.debug(`Nothing to do for activity %s`, activity._id);
            return false;
        }

        // compute which files need to be appended
        const activityFiles = await activityState.files();
        const { added: filesToAdd } = diffArray(activityFiles, traceFiles);
        const nowDate = now();
        const elapsedTime = duration(activityState.lastUpdate, nowDate);
        if (filesToAdd.length < this.#opts.batchSize && elapsedTime < this.#opts.maxDelay) {
            const durationStr = formatDuration(elapsedTime);
            logger.debug(`Update postponed elapsedTime=%s, batchSize=%d for activity `, durationStr, filesToAdd.length, activity._id);
            return false;
        }

        logger.info(`Compacting activity %s`, activity._id);
        await activityState.update(filesToAdd, nowDate, sha1);
        return true;
    }

    /**
     * 
     * @param {Activity} activity 
     * @param {ActivityCompactionState} activityState 
     */
    async #distributeTraceToOwners(activity, activityState) {
        const localStatePath = activityState.localStatePath;
        const usersDir = this.#opts.minio.users_dir;
        const tracesFilename = this.#opts.minio.traces_file;
        for(const username of activityState.owners) {
            const remotePath = `${usersDir}/${username}/${activityState.activityId}/${tracesFilename}`;
            this.#minio.copyToRemoteFile(localStatePath, remotePath);
        }
        logger.info(`Copied compacted file for activity %s to owners %s`, activity._id, activityState.owners.join(', '));
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
