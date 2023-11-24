import { now, duration, formatDuration } from './utils/date.js';
import { logger } from './logger.js';
import { MinioClient } from './minio.js';
import { SimvaClient } from './simva.js';
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

        const usersDir = this.#opts.minio.users_dir;
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
            for(const username of activityState.owners) {
                const remotePath = `${usersDir}/${username}/${activityState.activityId}/${tracesFilename}`;
                if (! await this.#minio.fileExists(remotePath) ) {
                    logger.warn('User \'%s\' compact file for activity \'%s\' not found: %s', username, activityState.activityId, remotePath);
                    consistent = false;
                }
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
            const activityState = state.get(activity);
            await this.#updateOwners({_id: activity, owners: []}, activityState);
            await state.remove(activity);
            logger.info('Removed %s', activity);
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
                    await this.#updateOwners({_id: activityId, owners: []}, activityState);
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
     * @param {Activity} activity 
     * @param {ActivityCompactionState} activityState 
     */
    async #updateOwners(activity, activityState) {
        const diffOwners = diffArray(activityState.owners, activity.owners.sort());
        // Remove files
        if (diffOwners.removed.length > 0) {
            const usersDir = this.#opts.minio.users_dir;
            const tracesFilename = this.#opts.minio.traces_file;
            if (this.#opts.removeDryRun) {
                logger.info('DRY RUN - Removing owners [%s] for activity: %s', diffOwners.removed.join(', '), activity._id);
                for(const removedOwner of diffOwners.removed) {
                    const remotePath = `${usersDir}/${removedOwner}/${activityState.activityId}/${tracesFilename}`;
                    logger.debug('DRY RUN - Removed remote file: %s', remotePath);
                }   
            } else {
                logger.info('Removing owners [%s] for activity: %s', diffOwners.removed.join(', '), activity._id);
                for(const removedOwner of diffOwners.removed) {
                    const remotePath = `${usersDir}/${removedOwner}/${activityState.activityId}/${tracesFilename}`;
                    await this.#minio.removeRemoteFile(remotePath);
                    logger.debug('Removed remote file: %s', remotePath);
                }    
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
            logger.debug(`Update postponed elapsedTime=%s, batchSize=%d for activity %s`, durationStr, filesToAdd.length, activity._id);
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
            await this.#minio.copyToRemoteFile(localStatePath, remotePath);
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
