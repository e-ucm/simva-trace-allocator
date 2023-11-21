import { MinioClient } from './minio.js';
import { logger } from './logger.js';
import { join } from 'node:path';
import { access, constants, copyFile, mkdir, open, readdir, rm, unlink } from 'node:fs/promises';
import { mktempPath } from './fileUtils.js';
import { duration, epoch, formatDuration, now, parseDate } from './dateUtils.js';
import { diffArray, diffSet } from './utils.js';

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
		try {
			await access(activityStatePath, constants.R_OK | constants.W_OK);
		} catch(e) {
			await mkdir(activityStatePath);
		}
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
		const localHashesAndFiles = await this.#listLocalHashes();
		const remoteHashesAndFiles = await this.#listRemoteHashes();

		const localHashes = new Set(localHashesAndFiles.keys());
		const remoteHashes = new Set(remoteHashesAndFiles.keys());
		const setsDiff = diffSet(localHashes, remoteHashes);
		if (setsDiff.added.length > 0 || setsDiff.removed.length > 0) {
			logger.warn('Local and remote folders for activity not synced: %s', this.activityId);
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
		const files = await readdir(activityPath, { withFileTypes: true});
		for(const file of files) {
			if (file.isFile()) {
				const chunks = file.name.split('-');
				const hash = chunks[0];
				const entry = hashes.get(hash);
				if (entry) {
					entry.push(file.name);
				} else {
					hashes.set(hash, [file.name]);
				}
			}
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
			logger.debug('DRY RUN - Removed remote files: %s', files.join(',\n'));
		} else {
			await this.#minio.removeRemoteFiles(files);
			logger.debug('Removed remote file: %s', files.join(',\n'));
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
				await rm(localPath);
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
			logger.debug('DRY RUN - Removed all remote files for activity %s: %s', this.activityId, files.join(',\n'));
		} else {
			await this.#minio.removeRemoteFiles(files);
			logger.debug('Removed all remote files for activity %s: %s', this.activityId, files.join(',\n'));
		}
	}

	async #clearLocalFiles() {
		const activityStatePath = join(this.#opts.localStatePath, this.activityId);
		if (this.#opts.removeDryRun) {
			logger.debug('DRY RUN - Removed all local files for activity %s: %s', this.activityId, activityStatePath);
		} else {
			await rm(activityStatePath, {force: true, recursive: true});
			logger.debug('Removed all remote files for activity %s: %s', this.activityId,activityStatePath);
		}
	}

	/**
	 * 
	 * @returns 
	 */
	async #loadLocalFilesState() {
		/** @type {string[]} */
		let files;
		const filesStatePath = this.#filesStateLocalPath();

		let filesState;
		try {
			filesState = await open(filesStatePath);
			for await (const line of filesState.readLines()) {
				files.push(line);
			}
		} catch (e) {
			if (filesState === undefined) {
				// Just warn, the file may not exist
				logger.warn(e);
			} else {
				throw e;
			}
		} finally {
			if (filesState !== undefined) {
				await filesState.close();
			}
		}
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
		const tmpPath = await mktempPath();
		if (this.currentSha1 !== null && this.currentSha1 !== sha1) {
			const currentStatePath = this.#stateLocalPath();
			try {
				await access(currentStatePath, constants.F_OK);
			} catch (e) {
				const currentStateRemotePath = this.#stateRemotePath();
				await this.#minio.copyFromRemoteFile(currentStateRemotePath, currentStatePath);
			}
			await copyFile(currentStatePath, tmpPath, constants.COPYFILE_EXCL);
		}
		let state;
		try {
			state = await open(tmpPath, 'a');
			for (const file of filesToAdd) {
				const content = await this.#minio.getFile(file);
				await state.write(content);
			}
			// XXX rename does not work if statePath is bind mounted
			await copyFile(tmpPath, statePath);
			await unlink(tmpPath);
		} catch (e) {
			throw e;
		} finally {
			if (state !== undefined) {
				await state.close();
			}
		}
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
		const tmpPath = await mktempPath();
		if (this.currentSha1 !== null && this.currentSha1 !== sha1) {
			const currentFilesStatePath = this.#filesStateLocalPath();
			await copyFile(currentFilesStatePath, tmpPath, constants.COPYFILE_EXCL);
		}
		let filesState;
		try {
			filesState = await open(tmpPath, 'a');
			for (const line of filesToAdd) {
				await filesState.write(line+'\n');
			}
			// XXX rename does not work if statePath is bind mounted
			await copyFile(tmpPath, filesStatePath);
			await unlink(tmpPath);
		} catch (e) {
			throw e;
		} finally {
			if (filesState !== undefined) {
				await filesState.close();
			}
		}
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
			await activity.garbageCollect();
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
		let file;
		try {
			file = await open(path);
			const content = await file.readFile('utf-8');
			this.#initState(content);
			return true;
		} catch(e) {
			logger.warn(e);
		} finally {
			if (file !== undefined) {
				await file.close();
			}
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
			const content = await this.#minio.getFile(this.#remotePath);
			this.#initState(content);
			return true;
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
	#initState(content) {
		const serializedState = /** @type {SerializedCompactorState} */(JSON.parse(content, withContextReviver(this.#opts, this.#minio)));
		this.#states = serializedState.states;
		this.#lastGC = serializedState.lastGC !== null ? new Date(Date.parse(serializedState.lastGC)) : null;
	}

	async save() {
		/** @type {SerializedCompactorState} */
		const serializedState = {
			states: this.#states,
			lastGC: this.#lastGC !== null ? this.#lastGC.toISOString() : null
		}
		const content = JSON.stringify(serializedState, replacer);
		await this.#saveLocalState(content);
		await this.#saveRemoteState(content);
	}

	/**
	 * 
	 * @param {string} content 
	 * @returns 
	 */
	async #saveRemoteState(content) {
		return this.#minio.setFile(this.#remotePath, content);
	}

	/**
	 * 
	 * @param {string} content 
	 * @returns 
	 */
	async #saveLocalState(content) {
		const path = this.#localPath;
		let file;
		try {
			file = await open(path, 'w');
			await file.writeFile(content);
		} finally {
			if (file !== undefined) {
				await file.close();
			}
		}
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
			activityState.clear();
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
