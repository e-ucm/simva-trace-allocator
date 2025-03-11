import { MinioClient } from './minio.js';
import { logger } from './logger.js';
import { join } from 'node:path';
import { copyNoOverwrite, ensureDirectoryStructureExists, fileExists, forceRemove, listFiles, mktempPath, rename, withFile } from './utils/file.js';
import { areArraysEqual, isStringArray, getFirstAndLastX } from './utils/array.js';
import { duration, epoch, formatDuration, now, parseDate } from './utils/date.js';
import { binarySearch, diffArray, diffSet } from './utils/misc.js';

/** @typedef {import('./config.js').CompactorOptions} CompactorOptions */

/**
 * @typedef SerializedCompactorState
 * @property {string} lastGC
 * @property {string} version
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
		this.currentSha1 = null;
		this.lastUpdate = epoch();
	}

	/** @type {CompactorOptions} opts */
	#opts;

	/** @type {MinioClient} */
	#minio;

	/** @type {string} */
	activityId;

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

	/**
	 * 
	 * @param {string[]} files 
	 * @returns {Promise<string[]>}
	 */
	async insertOrdered(files) {
		let activityFiles = await this.files();
		for(let i in files) {
			let file = files[i];
			let positionvalue=-binarySearch(activityFiles, file, true, (a,b)=> { 
				if(typeof a == "string" && typeof b == "string" ) {
					return a.localeCompare(b); 
				} else {
					return -1;
				}
			})-1;

			const nextposition = activityFiles.length;
			logger.debug(file);
			logger.debug("positionvalue:");
			logger.debug(positionvalue);
			logger.debug("nextposition:");
			logger.debug(nextposition);
			
			if(positionvalue < nextposition) {
				logger.warn("Not ordered. Should have been consumed before.")
				activityFiles.splice(positionvalue, 0, file);
			} else {
				activityFiles.push(file);
			}
			logger.debug(activityFiles);
		}
		return activityFiles;
	}

	async garbageCollect() {
		if (this.currentSha1 === undefined) {
			return 0;
		}
		const localHashesAndFiles = await this.#listLocalHashes();
		const remoteHashesAndFiles = await this.#listRemoteHashes();
		localHashesAndFiles.forEach((value, key) => {
			logger.debug(`${key}: ${value.join(", ")}`);
		});
		remoteHashesAndFiles.forEach((value, key) => {
			logger.debug(`${key}: ${value.join(", ")}`);
		});
		const localHashes = new Set(localHashesAndFiles.keys());
		const remoteHashes = new Set(remoteHashesAndFiles.keys());
		const setsDiff = diffSet(localHashes, remoteHashes);
		logger.debug(setsDiff);
		/** @type {string[]} */
		let localFilesToRemove = [];
		/** @type {string[]} */
		let remoteFilesToRemove = [];
		if (setsDiff.added.length > 0 || setsDiff.removed.length > 0) {
			logger.warn('Local and remote folders for activity not synced for activity %s', this.activityId);
			if(setsDiff.added.includes(this.currentSha1)) {
				logger.warn('%s current local file not present: %s', this.currentSha1, this.activityId);
				logger.info('garbage collection skipped: %s', this.activityId);
				return;
				//this.#minio.copyFromRemoteFile(this.#filesStateRemotePath(), this.#filesStateLocalPath());
				//this.#minio.copyFromRemoteFile(this.#stateRemotePath(), this.#stateLocalPath());
				//setsDiff.added= setsDiff.added.filter(sha1 => (sha1 !== this.currentSha1));
			} else if(setsDiff.removed.includes(this.currentSha1)) {
				logger.warn('%s current remote file not present: %s', this.currentSha1, this.activityId);
				logger.info('garbage collection skipped: %s', this.activityId);
				return;
				//this.#minio.copyToRemoteFile(this.#filesStateLocalPath(), this.#filesStateRemotePath());
				//this.#minio.copyToRemoteFile(this.#stateLocalPath(), this.#stateRemotePath());
				//setsDiff.removed= setsDiff.removed.filter(sha1 => (sha1 !== this.currentSha1));
			} else {
				let localFileContent= (await this.#loadLocalState());
				localFileContent.push("");
				logger.debug("localFileContent:");
				logger.debug(getFirstAndLastX(localFileContent, 3));
				let remoteFileContent= (await this.#minio.getFile(this.#stateRemotePath())).split('\n');
				logger.debug("remoteFileContent:");
				logger.debug(getFirstAndLastX(remoteFileContent, 3));
				if(areArraysEqual(localFileContent, remoteFileContent)) {
					logger.info("Local file is the same that the remote file.");
 				} else {
					logger.warn("Remote file is different that the local file.");
					logger.warn('garbage collection skipped: %s', this.activityId);
					return;
					//this.#minio.copyFromRemoteFile(this.#filesStateRemotePath(), this.#filesStateLocalPath());
					//this.#minio.copyFromRemoteFile(this.#stateRemotePath(), this.#stateLocalPath());
				}
			}
			if(setsDiff.added.length > 0) {
				setsDiff.added.forEach((hash) => {
					const remoteFiles = remoteHashesAndFiles.get(hash);
					if(isStringArray(remoteFiles)) {
						remoteFilesToRemove = remoteFilesToRemove.concat(remoteFiles);
					}
				});
			}

			if(setsDiff.removed.length > 0) {
				setsDiff.removed.forEach((hash) => {
					const localFiles = localHashesAndFiles.get(hash);
					if(isStringArray(localFiles)) {
						localFilesToRemove = localFilesToRemove.concat(localFiles);
					}
				});
			}
		}

		localHashes.forEach((hash) => {
			if(hash !== this.currentSha1) {
				const localFiles = localHashesAndFiles.get(hash);
				if(isStringArray(localFiles)) {
					localFilesToRemove = localFilesToRemove.concat(localFiles);
				}
				const remoteFiles = remoteHashesAndFiles.get(hash);
				if(isStringArray(remoteFiles)) {
					remoteFilesToRemove = remoteFilesToRemove.concat(remoteFiles);
				}
			}
		});
		
		await this.#removeRemoteFiles(remoteFilesToRemove);
		await this.#removeLocalFiles(localFilesToRemove);
		return 0;
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
			const filePath= join(activityPath, file);
			const chunks = file.split('-');
			const hash = chunks[0];
			const entry = hashes.get(hash);
			if (entry) {
				entry.push(filePath);
			} else {
				hashes.set(hash, [filePath]);
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
			const hash = chunks[0].replace(remotePath, "");
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
			if (this.#opts.removeDryRun) {
				logger.debug('DRY RUN - Removed local file: %s', file);
			} else {
				await forceRemove(file);
				logger.debug('Removed local file: %s', file);
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
			logger.debug('Removed all local files for activity %s: %s', this.activityId,activityStatePath);
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

	/**
	 * 
	 * @returns 
	 */
	#outputRemotePath() {
		const path = join(this.#opts.minio.outputs_dir, this.activityId,this.#opts.minio.traces_file);
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

		await withStateFile(async (state) => {
			for (const file of filesToAdd) {
				const content = await this.#minio.getFile(file);
				await state.write(content);
			}
		});
		await rename(tmpPath, statePath, this.#opts.copyInsteadRename);
	}


	/**
	 * 
	 * @returns 
	 */
	async #loadLocalState() {
		const statePath = this.#stateLocalPath();
		const withState = withFile(statePath);

		/** @type {string[]} */
		const files = await withState(async (state) => {
			/** @type {string[]} */
			const files=[];
			for await (const line of state.readLines()) {
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
	 * @returns
	 */
	get remoteStatePath() {
		return this.#stateRemotePath();
	}

	/**
	 * @returns
	 */
	get remoteOutputPath() {
		return this.#outputRemotePath();
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
		const stateFiles = (await this.insertOrdered(filesToAdd)).join('\r\n');
		const withFilesState = withFile(tmpPath, 'a');
		await withFilesState(async (file) => {
			await file.writeFile(stateFiles);
		})
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
		this.#version = null;
	}
	/** @type {CompactorOptions} opts */
	#opts;

	/** @type {MinioClient} */
	#minio;

	/** @type {Map<string, ActivityCompactionState>} */
	#states;

	/** @type {Date} */
	#lastGC;

	/** @type {Number} */
	#version;

	async init() {
		logger.debug("Loading Local State...")
		let localStateLoaded = await this.#loadLocalState(false);
		let localStateVersion=this.#version;
		logger.debug("Loading Remote State...");
		let remoteStateLoaded = await this.#loadRemoteState(true);
		let remoteStateVersion=this.#version;
		if (!localStateLoaded && !remoteStateLoaded) {
			logger.warn('Seems that we are running for the first time');
			return;
		}
		logger.debug(`Local version : ${localStateVersion } - Remote version : ${remoteStateVersion }`)
		if(remoteStateVersion > localStateVersion) {
			logger.debug("The version in remote is more uptodate. Taking this version.");
			await this.#loadRemoteState(false);
			return;
		} else if(remoteStateVersion < localStateVersion) {
			logger.debug("The version in local is more uptodate. Taking this version.");
			await this.#loadLocalState(false);
			await this.#copyStateToRemote();
			return;
		} else {
			logger.debug("The versions in local and in remote are the same.");
			return;
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
        let activityToPass=[];
		const nowDate = now();
		const lastGC = this.#lastGC ?? epoch();
		const elapsedTimeSinceLastGC = duration(lastGC, nowDate);
		if (elapsedTimeSinceLastGC < this.#opts.gcInterval) {
			return [];
		}

		logger.debug('Garbage collection started');
		for(const activity of this.#states.values()) {
			try {
				let result = await activity.garbageCollect();
				logger.info("activity.garbageCollect : " + result);
				switch(result) {
					case 1:
						logger.warn("Error during garbage collection.");
						if(this.#opts.tryRecovery) {
							logger.warn("Removing activity :" + activity.activityId);
							await activity.clear();
							await this.remove(activity.activityId);
						}
						activityToPass.push(activity.activityId);
						break;
					default:
						logger.info("Everything ok during garbage collection.");		 
				}
			} catch (error) {
				logger.error('Could not garbage collect: %s', activity.activityId);
				logger.error(error);
			}
		}
		const finishTime = now();
		logger.info('Garbage collection finished, took: %s', formatDuration(duration(nowDate, finishTime)));
		this.#lastGC = finishTime;
		return activityToPass;
	}

	/**
	 * @param {boolean } loadTemp
	 * @return {Promise<boolean>} true if config has been loaded
	 */
	async #loadLocalState(loadTemp) {
		let path;
		if(loadTemp) {
			path = this.#localTempPath;
		} else {
			path = this.#localPath;
		}
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

	get #localTempPath () {
		const path = join(this.#opts.localStatePath, `temp_${STATE_FILENAME}`);
		return path;
	}

	/**
	 * @param {boolean} loadTemp
	 * @return {Promise<boolean>} true if config has been loaded
	 */
	async #loadRemoteState(loadTemp) {
		let path;
		if(loadTemp) {
			path = this.#localTempPath;
		} else {
			path = this.#localPath;
		}
		try {
			await this.#minio.copyFromRemoteFile(this.#remotePath, path);
			return this.#loadLocalState(loadTemp);
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
		this.#version = serializedState.version !== null ? parseInt(serializedState.version) : 0 ;
	}

	async save() {
		this.#version =this.#version+1;
		/** @type {SerializedCompactorState} */
		const serializedState = {
			version: this.#version.toString(),
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
				currentSha1 : value.currentSha1
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
				return activityState;
			}
		}
		return value;
	}
}
