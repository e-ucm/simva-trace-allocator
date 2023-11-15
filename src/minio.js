import { Client } from 'minio';
import { logger } from './logger.js';

/**
 * @typedef MinioOpts
 * @property {string} host
 * @property {boolean} [useSSL]
 * @property {number} [port]
 * @property {string} accessKey
 * @property {string} secretKey
 * @property {string} bucket
 * @property {string} topics_dir
 * @property {string} users_dir
 * @property {string} traces_file
 */

export class MinioClient {

    /**
     * @param {MinioOpts} opts
     */
    constructor(opts) {
        this.#opts = opts;
        this.#minio = new Client({
            endPoint: opts.host,
            port: opts.port,
            useSSL: opts.useSSL,
            accessKey: opts.accessKey,
            secretKey: opts.secretKey
        });
    }
    /** @type {MinioOpts} */
    #opts;

    /** @type {Client} */
    #minio;

    /**
     * @param {string} folder
     * 
     * @returns {Promise<unknown[]>}
     */
    async listFiles(folder){
        let objectsStream = this.#minio.listObjects(this.#opts.bucket, folder, false);
        const files = [];
        for await(const chunk of objectsStream) {
            files.push(chunk);
        }
        return files;
    }

    /**
     * 
     * @param {string} file 
     * @returns {Promise<string>}
     */
    async getFile(file){
        let objectStream = (await this.#minio.getObject(this.#opts.bucket, file)).setEncoding('utf-8');
        let content = '';
        for await(const chunk of objectStream) {
            content += chunk;
        }
        return content;
    }

    /**
     * 
     * @param {string} remotePath 
     * @param {string} localPath 
     * @returns
     */
    async copyFromRemoteFile(remotePath, localPath){
        return await this.#minio.fGetObject(this.#opts.bucket, remotePath, localPath);
    }

    /**
     * 
     * @param {string} remotePath 
     * @param {string} localPath
     * @returns 
     */
	async copyToRemoteFile(localPath, remotePath) {
        await this.#minio.fPutObject(this.#opts.bucket, remotePath, localPath);
	}

    /**
     * 
     * @param {string} file 
     * @param {string} content 
     * @returns 
     */
    async setFile(file, content) {
        const info = this.#minio.putObject(this.#opts.bucket, file, content);
        return info;
    }

    /**
     * 
     * @param {string} activityId 
     * @returns 
     */
    async getTraces(activityId){
        return this.listFiles(`/${this.#opts.topics_dir}/traces/_id=${activityId}/`);
    }

    /**
     * 
     * @param {string} activityId 
     * @param {string} owner 
     */
	async removeCompactedFileForUser(activityId, owner) {
        const tracesfile = `${this.#opts.users_dir}/${owner}/${activityId}/${this.#opts.traces_file}`;
        logger.debug(`Simulate removing: %s`, tracesfile);
	}

}


function streamToString(stream) {
	const chunks = []
	return new Promise((resolve, reject) => {
		try {
			stream.on('data', chunk => chunks.push(chunk))
			stream.on('error', reject)
			stream.on('end', () => resolve(Buffer.concat(chunks).toString('utf8')))
		} catch (e) {
			reject(e);
		}
	})
}

