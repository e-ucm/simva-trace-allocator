import { MinioClient } from './minio.js';
import { logger } from './logger.js';
import { join } from 'node:path';
import { access, constants, copyFile, mkdir, open, unlink } from 'node:fs/promises';
import { mktempPath } from './fileUtils.js';
import { epoch, parseDate } from './dateUtils.js';

/** @typedef {import('./config.js').CompactorOptions} CompactorOptions */

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
	
class CompactorState {
	/**
	 * @param {CompactorOptions} opts 
	 * @param {MinioClient} minio 
	 */
	constructor(opts, minio) {
		this.#opts = opts;
		this.#minio = minio;
		this.#states = new Map();
	}
	/** @type {CompactorOptions} opts */
	#opts;

	/** @type {MinioClient} */
	#minio;

	/** @type {Map<string, ActivityCompactionState>} */
	#states;

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
		this.#states = /** @type {Map<string, ActivityCompactionState>} */(JSON.parse(content, withContextReviver(this.#opts, this.#minio)));
	}

	async save() {
		const content = JSON.stringify(this.#states, replacer);
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
