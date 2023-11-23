import { MinioClient } from './minio.js';
import { logger } from './logger.js';
import { join } from 'node:path';
import { copyNoOverwrite, ensureDirectoryStructureExists, fileExists, forceRemove, listFiles, mktempPath, rename, withFile } from './utils/file.js';
import { duration, epoch, formatDuration, now, parseDate } from './utils/date.js';
import { diffArray, diffSet } from './utils/misc.js';

/** @typedef {import('./config.js').CompactorOptions} CompactorOptions */

/**
 * @typedef SerializedCompactorState
 * @property {string} lastGC
 * @property {Map<string, ActivityCompactionState>} states
 */

export class ActivityCompactionState {

	/**
	 * 
	 * @param {string} activityId
	 * @param {CompactorOptions} opts 
	 * @param {MinioClient} minio 
	 */
	constructor(activityId, opts, minio) {
		this.activityId = activityId;
		this.#opts = opts;
		this.#minio = minio;
		this.owners = [];
		this.currentSha1 = null;
		this.lastUpdate = epoch();
	}

	/** @type {CompactorOptions} opts */
	#opts;

	/** @type {MinioClient} */
	#minio;

	/** @type {string} */
	activityId;

	/** @type {string[]} */
	owners;

	/** @type {string} */
	currentSha1;

	/** @type {Date} */
	lastUpdate;

	async init() {
		const activityStatePath = join(this.#opts.localStatePath, this.activityId);
		await ensureDirectoryStructureExists(activityStatePath);
	}

	/**
	 * @returns {Promise<string[]>}
	 */
	async files() {
		if (this.currentSha1 === null) return [];

		let files = await this.#loadLocalFilesState();
		if (files !== undefined) return files;

		await this.#copyFromRemoteFilesState();

		files = await this.#loadLocalFilesState();
		return files;
	}

	async garbageCollect() {
		if (this.currentSha1 === undefined) {
			return;
		}
		const localHashesAndFiles = await this.#listLocalHashes();
		const remoteHashesAndFiles = await this.#listRemoteHashes();

		const localHashes = new Set(localHashesAndFiles.keys());
		const remoteHashes = new Set(remoteHashesAndFiles.keys());
		const setsDiff = diffSet(localHashes, remoteHashes);
		if (setsDiff.added.length > 0 || setsDiff.removed.length > 0) {
			logger.warn('Local and remote folders for activity not synced, garbage collection skipped: %s', this.activityId);
			return;
		}

		/** @type {string[]} */
		let localFilesToRemove = [];
		/** @type {string[]} */
		let remoteFilesToRemove = [];
		localHashes.forEach((hash) => {
			if(hash !== this.currentSha1) {
				const localFiles = localHashesAndFiles.get(hash);
				localFilesToRemove = localFilesToRemove.concat(localFiles);
				const remoteFiles = remoteHashesAndFiles.get(hash);
				remoteFilesToRemove = remoteFilesToRemove.concat(remoteFiles);
			}
		});
		setsDiff.added.forEach((hash) => {
			const remoteFiles = remoteHashesAndFiles.get(hash);
			remoteFilesToRemove = remoteFilesToRemove.concat(remoteFiles);
		});
		await this.#removeRemoteFiles(remoteFilesToRemove);
		await this.#removeLocalFiles(localFilesToRemove);
	}

	/**
	 * 
	 * @returns {Promise<Map<string, string[]>>}
	 */
	async #listLocalHashes() {
		/** @type {Map<string, string[]>} */
		const hashes = new Map();
		const activityPath = join(this.#opts.localStatePath, this.activityId);
		const files = await listFiles(activityPath);
		for(const file of files) {
			const chunks = file.split('-');
			const hash = chunks[0];
			const entry = hashes.get(hash);
			if (entry) {
				entry.push(file);
			} else {
				hashes.set(hash, [file]);
			}
		}
		if (this.currentSha1 !== undefined && hashes.size < 2 ) {

		}
		return hashes;
	}

	/**
	 * 
	 * @returns {Promise<Map<string, string[]>>}
	 */
	async #listRemoteHashes() {
		/** @type {Map<string, string[]>} */
		const hashes = new Map();
		const remotePath = `${this.#opts.remoteStatePath}/${this.activityId}/`;
		const files = await this.#minio.listFiles(remotePath);
		for(const file of files) {
			const chunks = file.name.split('-');
			const hash = chunks[0];
			const entry = hashes.get(hash);
			if (entry) {
				entry.push(file.name);
			} else {
				hashes.set(hash, [file.name]);
			}
		}
		return hashes;
	}

	/**
	 * 
	 * @param {string[]} files 
	 */
	async #removeRemoteFiles(files) {
		files = files.map((file) => `${this.#opts.remoteStatePath}/${this.activityId}/${file}`);
		if (this.#opts.removeDryRun) {
			logger.debug('DRY RUN - Removed remote files:\n %s', files.join(',\n'));
		} else {
			await this.#minio.removeRemoteFiles(files);
			logger.debug('Removed remote file:\n %s', files.join(',\n'));
		}
	}

	/**
	 * 
	 * @param {string[]} files 
	 */
	async #removeLocalFiles(files) {
		for(const file of files) {
			const localPath = join(this.#opts.localStatePath, this.activityId, file);
			if (this.#opts.removeDryRun) {
				logger.debug('DRY RUN - Removed local file: %s', localPath);
			} else {
				await forceRemove(localPath);
				logger.debug('Removed local file: %s', localPath);
			}
		}
	}

	async clear() {
		await this.#clearRemoteFiles();
		await this.#clearLocalFiles();
	}

	async #clearRemoteFiles() {
		const fileEntries = await this.#minio.listFiles(`${this.#opts.remoteStatePath}/${this.activityId}/`);
		const files = fileEntries.map((e) => e.name);
		if (this.#opts.removeDryRun) {
			logger.debug('DRY RUN - Removed all remote files for activity %s:\n %s', this.activityId, files.join(',\n'));
		} else {
			await this.#minio.removeRemoteFiles(files);
			logger.debug('Removed all remote files for activity %s:\n %s', this.activityId, files.join(',\n'));
		}
	}

	async #clearLocalFiles() {
		const activityStatePath = join(this.#opts.localStatePath, this.activityId);
		if (this.#opts.removeDryRun) {
			logger.debug('DRY RUN - Removed all local files for activity %s: %s', this.activityId, activityStatePath);
		} else {
			await forceRemove(activityStatePath);
			logger.debug('Removed all remote files for activity %s: %s', this.activityId,activityStatePath);
		}
	}

	/**
	 * 
	 * @returns 
	 */
	async #loadLocalFilesState() {
		const filesStatePath = this.#filesStateLocalPath();
		const withFileState = withFile(filesStatePath);

		/** @type {string[]} */
		const files = await withFileState(async (fileState) => {
			/** @type {string[]} */
			const files=[];
			for await (const line of fileState.readLines()) {
				files.push(line);
			}
			return files;
		}, false);

		return files;
	}

	/**
	 * 
	 * @param {string} [sha1]
	 * @returns 
	 */
	#filesStateLocalPath(sha1) {
		sha1 = sha1 || this.currentSha1;
		const path = join(this.#opts.localStatePath, this.activityId, `${sha1}-files.txt`);
		return path;
	}

	async #copyFromRemoteFilesState() {
		const remotePath = this.#filesStateRemotePath();
		const localPath = this.#filesStateLocalPath();
		await this.#minio.copyFromRemoteFile(remotePath, localPath);
	}

	/**
	 * 
	 * @param {string} [sha1]
	 * @returns 
	 */
	#filesStateRemotePath(sha1) {
		sha1 = sha1 || this.currentSha1;
		return `${this.#opts.remoteStatePath}/${this.activityId}/${sha1}-files.txt`;
	}

	/**
	 * 
	 * @param {string[]} filesToAdd 
	 * @param {Date} now 
	 * @param {string} sha1 
	 */
    async update(filesToAdd, now, sha1) {
		await this.#saveLocalState(filesToAdd, sha1);
		await this.#copyToRemoteState(sha1);
		await this.#saveLocalFilesState(filesToAdd, sha1);
		await this.#copyToRemoteFilesState(sha1);
		this.currentSha1 = sha1;
		this.lastUpdate = now;
    }
	/**
	 * 
	 * @param {string[]} filesToAdd 
	 * @param {string} sha1 
	 * @returns 
	 */
	async #saveLocalState(filesToAdd, sha1) {
		const statePath = this.#stateLocalPath(sha1);
		const tmpPath = mktempPath();
		if (this.currentSha1 !== null && this.currentSha1 !== sha1) {
			const currentStatePath = this.#stateLocalPath();
			if (!await fileExists(currentStatePath)) {
				const currentStateRemotePath = this.#stateRemotePath();
				await this.#minio.copyFromRemoteFile(currentStateRemotePath, currentStatePath);
			}
			await copyNoOverwrite(currentStatePath, tmpPath);
		}
		const withStateFile = withFile(tmpPath, 'a');
		withStateFile(async (state) => {
			for (const file of filesToAdd) {
				const content = await this.#minio.getFile(file);
				await state.write(content);
			}
		});
		await rename(tmpPath, statePath, this.#opts.copyInsteadRename);
	}

	/**
	 * 
	 * @param {string} [sha1]
	 * @returns 
	 */
	#stateLocalPath(sha1) {
		sha1 = sha1 || this.currentSha1;
		const path = join(this.#opts.localStatePath, this.activityId, `${sha1}-state.txt`);
		return path;
	}

	/**
	 * @returns
	 */
	get localStatePath() {
		return this.#stateLocalPath();
	}

	/**
	 * 
	 * @param {string} [sha1]
	 * @returns 
	 */
	#stateRemotePath(sha1) {
		sha1 = sha1 || this.currentSha1;
		return `${this.#opts.remoteStatePath}/${this.activityId}/${sha1}-state.txt`;
	}

	/**
	 * 
	 * @param {string[]} filesToAdd 
	 * @param {string} sha1 
	 * @returns 
	 */
	async #saveLocalFilesState(filesToAdd, sha1) {
		const filesStatePath = this.#filesStateLocalPath(sha1);
		const tmpPath = mktempPath();
		if (this.currentSha1 !== null && this.currentSha1 !== sha1) {
			const currentFilesStatePath = this.#filesStateLocalPath();
			await copyNoOverwrite(currentFilesStatePath, tmpPath);
		}
		const withFilesState = withFile(tmpPath, 'a');
		await withFilesState(async (filesState) => {
			for (const line of filesToAdd) {
				await filesState.write(line+'\n');
			}
		});
		await rename(tmpPath, filesStatePath, this.#opts.copyInsteadRename);
	}

	/**
	 * 
	 * @param {string} sha1 
	 */
	async #copyToRemoteFilesState(sha1) {
		const localPath = this.#filesStateLocalPath(sha1);
		const remotePath = this.#filesStateRemotePath(sha1);
		await this.#minio.copyToRemoteFile(localPath, remotePath);
	}

	/**
	 * 
	 * @param {string} sha1 
	 */
	async #copyToRemoteState(sha1) {
		const localPath = this.#stateLocalPath(sha1);
		const remotePath = this.#stateRemotePath(sha1);
		await this.#minio.copyToRemoteFile(localPath, remotePath);
	}

	async checkConsistency() {
		if (this.currentSha1 === null)  {
			logger.debug('Activity new and consistent: %s', this.activityId);
			return true;
		}

		let consistent = true;
		const localStatePath = this.#stateLocalPath();
		const localFilesStatePath = this.#filesStateLocalPath();
		if (! await fileExists(localStatePath)) {
			logger.warn('Local state file for activity \'%s\' not found: %s', this.activityId, localStatePath);
			consistent = false;
		}
		if (! await fileExists(localFilesStatePath)) {
			logger.warn('Local files state for activity \'%s\' not found: %s', this.activityId, localFilesStatePath);
			consistent = false;
		}
		const remoteStatePath = this.#stateRemotePath();
		const remoteFilesStatePath = this.#filesStateRemotePath();
		if (! await this.#minio.fileExists(remoteStatePath)) {
			logger.warn('Remote state file for activity \'%s\' not found: %s', this.activityId, remoteStatePath);
			consistent = false;
		}
		if (! await this.#minio.fileExists(remoteFilesStatePath)) {
			logger.warn('Remote files state for activity \'%s\' not found: %s', this.activityId, remoteFilesStatePath);
			consistent = false;
		}

		return consistent;
	}
}

export const STATE_FILENAME = 'state.json';
	
export class CompactorState {
	/**
	 * @param {CompactorOptions} opts 
	 * @param {MinioClient} minio 
	 */
	constructor(opts, minio) {
		this.#opts = opts;
		this.#minio = minio;
		this.#states = new Map();
		this.#lastGC = null;
	}
	/** @type {CompactorOptions} opts */
	#opts;

	/** @type {MinioClient} */
	#minio;

	/** @type {Map<string, ActivityCompactionState>} */
	#states;

	/** @type {Date} */
	#lastGC;

	async init() {
		let loaded = await this.#loadLocalState();
		if (loaded) return;
		loaded = await this.#loadRemoteState();
		if (!loaded) {
			logger.warn('Seems that we are running for the first time');
		}
	}

	/**
	 * 
	 * @param {string} activityId 
	 * @returns 
	 */
	async create(activityId) {
		const activityState = new ActivityCompactionState(activityId, this.#opts, this.#minio);
		await activityState.init();
		this.#states.set(activityId, activityState);
		return activityState;
	}

	async garbageCollect() {
		const nowDate = now();
		const lastGC = this.#lastGC ?? epoch();
		const elapsedTimeSinceLastGC = duration(lastGC, nowDate);
		if (elapsedTimeSinceLastGC < this.#opts.gcInterval) {
			return;
		}

		logger.debug('Garbage collection started');
		for(const activity of this.#states.values()) {
			try {
				await activity.garbageCollect();
			} catch (error) {
				logger.error('Could not garbage collect: %s', activity.activityId);
				logger.error(error);
			}
		}
		const finishTime = now();
		logger.info('Garbage collection finished, took: %s', formatDuration(duration(nowDate, finishTime)));
		this.#lastGC = finishTime;
	}

	/**
	 * @return {Promise<boolean>} true if config has been loaded
	 */
	async #loadLocalState() {
		const path = this.#localPath;
		const withState = withFile(path);
		const result = await withState(async (file) => {
			const content = await file.readFile('utf-8');
			await this.#initState(content);
			return true;
		}, false);
		if (result !== undefined) {
			return result;
		}
		return false;
	}

	get #localPath () {
		const path = join(this.#opts.localStatePath, STATE_FILENAME);
		return path;
	}

	/**
	 * @return {Promise<boolean>} true if config has been loaded
	 */
	async #loadRemoteState() {
		try {
			await this.#minio.copyFromRemoteFile(this.#remotePath, this.#localPath);
			return this.#loadLocalState();
		} catch (e) {
			logger.warn(e);
		}
		return false;
	}

	get #remotePath() {
		return `${this.#opts.remoteStatePath}/${STATE_FILENAME}`;
	}

	/**
	 * 
	 * @param {string} content 
	 */
	async #initState(content) {
		const serializedState = /** @type {SerializedCompactorState} */(JSON.parse(content, withContextReviver(this.#opts, this.#minio)));
		this.#states = serializedState.states;
		for(const activity of this.#states.values()) {
			try {
				await activity.init();
			} catch (error) {
				logger.error('Could not initialize activity: ', activity.activityId);
				logger.error(error);
			}
		}
		this.#lastGC = serializedState.lastGC !== null ? new Date(Date.parse(serializedState.lastGC)) : null;
	}

	async save() {
		/** @type {SerializedCompactorState} */
		const serializedState = {
			states: this.#states,
			lastGC: this.#lastGC !== null ? this.#lastGC.toISOString() : null
		}
		const content = JSON.stringify(serializedState, replacer);
		await this.#saveStateLocal(content);
		await this.#copyStateToRemote();
	}

	/**
	 * 
	 * @returns 
	 */
	async #copyStateToRemote() {
		return this.#minio.copyToRemoteFile(this.#localPath, this.#remotePath);
	}

	/**
	 * 
	 * @param {string} content 
	 * @returns 
	 */
	async #saveStateLocal(content) {
		const path = this.#localPath;
		const withFileState = withFile(path, 'w');
		await withFileState(async (file) => {
			file.writeFile(content);
		})
	}

	get size() {
		return this.#states.size;
	}

	/**
	 * 
	 * @param {string} activityId 
	 */
	get(activityId) {
		const activity = this.#states.get(activityId);
		return activity;
	}

	/**
	 * 
	 * @param {string} activityId 
	 */
	async remove(activityId) {
		const activityState = this.#states.get(activityId);
		if (!activityState) return;

		if (this.#opts.removeDryRun) {
			logger.info('DRY RUN - Known activity removed: %s', activityId);
		} else {
			await activityState.clear();
			this.#states.delete(activityId);
			logger.info('Known activity removed: %s', activityId);
		}
	}

	/**
	 * @returns {IterableIterator<string>}
	 */
	get knownActivities() {
		const it = this.#states.keys();
		return it;
	}
}
/**
 * @param {CompactorOptions} opts 
 * @param {MinioClient} minio
 * @returns {Promise<CompactorState>}
 */
export async function getState(opts, minio) {
	/** @type {CompactorState} */
	let state = new CompactorState(opts, minio);
	await state.init();
	return state;
}

function replacer(key, value) {
	if (value instanceof Map) {
		return {
			dataType: 'Map',
			value: Array.from(value.entries()), // or with spread: value: [...value]
		};
	}
	if (value instanceof ActivityCompactionState) {
		return {
			dataType: 'ActivityCompactionState',
			value: {
				activityId : value.activityId,
				lastUpdate : value.lastUpdate.toISOString(),
				currentSha1 : value.currentSha1,
				owners : value.owners
			}
		}
	}
	return value;
}

/**
 * @param {CompactorOptions} opts 
 * @param {MinioClient} minio
 * @returns
 */
function withContextReviver(opts, minio) {
	return function reviver(key, value) {
		if (typeof value === 'object' && value !== null) {
			if (value.dataType === 'Map') {
				return new Map(value.value);
			}
			if (value.dataType === 'ActivityCompactionState') {
				value = value.value;
				const activityState = new ActivityCompactionState(value.activityId, opts, minio);
				activityState.lastUpdate = parseDate(value.lastUpdate);
				activityState.currentSha1 = value.currentSha1;
				activityState.owners = value.owners;
				return activityState;
			}
		}
		return value;
	}
}
