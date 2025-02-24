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
 * @property {string} traces_topic
 * @property {string} outputs_dir
 * @property {string} traces_file
 */

/**
 * @typedef ListEntry
 * @property {string} name name of the object.
 * @property {string} prefix name of the object prefix.
 * @property {number} size size of the object.
 * @property {string} etag etag of the object.
 * @property {string} versionId versionId of the object.
 * @property {boolean} isDeleteMarker true if it is a delete marker.
 * @property {Date} lastModified modified time stamp.
 */

/**
 * @typedef FPutResult
 * @property {string} etag etag of the object.
 * @property {string} versionId versionId of the object.
 */

/**
 * @typedef MultipartUploadResult
 * @property {string} uploadId uploadId of the object.
 * @property {string} key key path of the object.
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
     * @returns {Promise<ListEntry[]>}
     */
    async listFiles(folder){
        logger.debug(folder);
        let objectsStream = this.#minio.listObjectsV2(this.#opts.bucket, folder, false);
        /** @type {ListEntry[]} */
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
     * @returns {Promise<void>}
     */
    async copyFromRemoteFile(remotePath, localPath){
        return this.#minio.fGetObject(this.#opts.bucket, remotePath, localPath);
    }

    /**
     * 
     * @param {string} remotePath 
     * @param {string} localPath
     * @returns {Promise<FPutResult>}
     */
	async copyToRemoteFile(localPath, remotePath) {
        logger.debug(`Coping file ${localPath} to remote ${remotePath}`);
        return this.#minio.fPutObject(this.#opts.bucket, remotePath, localPath);
	}

    /**
     * 
     * @param {string} objectName 
     * @param {string} uploadId
     * @returns {Promise<void>}
     */
    async abortMultipartUpload(objectName, uploadId) {
        await this.#minio.abortMultipartUpload(this.#opts.bucket, objectName, uploadId);
    }

    /**
     * 
     * @param {MultipartUploadResult[]} list
     * @returns {Promise<void>}
     */
    async abortMultipartUploads(list) {
        for (const item of list) {
            try {
                await this.#minio.abortMultipartUpload(this.#opts.bucket, item.key, item.uploadId);
                logger.info(`Aborted upload: ${item.key} (Upload ID: ${item.uploadId})`);
            } catch (error) {
                logger.error(`Failed to abort ${item.key}:`, error);
            }
        }
    }
    

    /**
     * 
     * @returns {Promise<MultipartUploadResult[]>}
     */
    async listMultipartUploads() {
        return new Promise((resolve, reject) => {
          const uploads = [];
          
          const stream = this.#minio.listIncompleteUploads(this.#opts.bucket, "", true);
          
          stream.on("data", (obj) => {
            uploads.push(obj);
            logger.info("Active Upload:", obj);
          });
      
          stream.on("end", () => {
            resolve(uploads);
          });
      
          stream.on("error", (err) => {
            reject(err);
          });
        });
    }

    async listV2MultipartUploads() {
        this.#minio.listIncompleteUploads(this.#opts.bucket, "", true)
          .on("data", async (obj) => {
            logger.info(obj);
            await this.#minio.abortMultipartUpload(this.#opts.bucket, obj.key, obj.uploadId);
          })
          .on("error", (err) => logger.error(err));
      }

    /**
     * 
     * @param {string} file 
     * @param {string} content 
     * @returns {Promise<FPutResult>}
     */
    async setFile(file, content) {
        return this.#minio.putObject(this.#opts.bucket, file, content);
    }

    /**
     * 
     * @param {string} activityId 
     * @returns 
     */
    async getTraces(activityId){
        return this.listFiles(`${this.#opts.topics_dir}/${this.#opts.traces_topic}/_id=${activityId}/`);
    }

    /**
     * 
     * @param {string} path 
     * @returns {Promise<void>}
     */
	async removeRemoteFile(path) {
        logger.debug(`removeRemoteFile file ${path}`);
        return this.#minio.removeObject(this.#opts.bucket, path);
	}

    /**
     * 
     * @param {string[]} paths 
     * @returns {Promise<any>}
     */
	async removeRemoteFiles(paths) {
        return this.#minio.removeObjects(this.#opts.bucket, paths);
	}

    /**
     * 
     * @param {string} path 
     * @returns {Promise<boolean>}
     */
    async fileExists(path) {
        const objectsStream = this.#minio.listObjectsV2(this.#opts.bucket, path);
        const iterator = objectsStream[Symbol.asyncIterator]();
        const nextValue = await iterator.next();
        return ! nextValue.done;
    }

    
    /**
     * 
     * @param {string} sourcePath 
     * @param {string} destinationPath 
     * @returns {Promise<void>}
     */
    async copyWithinMinIO(sourcePath, destinationPath) {
        logger.debug(`Coping file into remote from ${sourcePath} to ${destinationPath}`);
        this.#minio.copyObject(
            this.#opts.bucket,
            destinationPath,
            `/${this.#opts.bucket}/${sourcePath}`
          );
      }
      

}

function streamToString(stream) {
	const chunks = []
	return new Promise((resolve, reject) => {
		try {
			stream.on('data', (chunk) => chunks.push(chunk))
			stream.on('error', reject)
			stream.on('end', () => resolve(Buffer.concat(chunks).toString('utf8')))
		} catch (e) {
			reject(e);
		}
	})
}

