/**
 * @fileoverview S3 client for MinIO / RustFS object storage.
 *
 * Wraps the AWS SDK v3 S3 client exposing the object storage API used by the
 * trace allocator (get/put/remove/exists/list/copy/presign) over an S3 compatible service.
 *
 * @module utils/minioclient
 * @requires @aws-sdk/client-s3
 * @requires @aws-sdk/s3-request-presigner
 */

import {
    S3Client,
    CopyObjectCommand,
    CreateBucketCommand,
    DeleteObjectCommand,
    DeleteObjectsCommand,
    GetObjectCommand,
    HeadBucketCommand,
    HeadObjectCommand,
    ListObjectsV2Command,
    PutObjectCommand,
} from "@aws-sdk/client-s3";
import { createReadStream, createWriteStream } from 'node:fs';
import { dirname } from 'node:path';
import { pipeline } from 'node:stream/promises';
import { logger } from './logger.js';
import { config } from './config.js';
import { NotFoundError } from './utils/errors.js';
import { ensureDirectoryStructureExists } from './utils/file.js';

/**
 * @typedef MinioOpts
 * @property {string} host endpoint used by the server to reach the object storage service.
 *  Inside the container network the external hostnames do not resolve, so this points
 *  to the internal service endpoint (i.e. rustfs.internal.test)
 * @property {boolean} [useSSL] whether the endpoint uses https
 * @property {number} [port] port of the object storage service
 * @property {string} [region]
 * @property {string} [publicHost] endpoint used only to sign the urls handed to the clients,
 *  falls back to {@link host} when not provided
 * @property {number} [publicPort]
 * @property {string} accessKey
 * @property {string} secretKey
 * @property {boolean} [forcePathStyle] RustFS uses path-style URLs by default;
 *  virtual-host style requires RUSTFS_SERVER_DOMAINS
 * @property {string} bucket
 * @property {string} topics_dir
 * @property {string} traces_topic
 * @property {string} outputs_dir
 * @property {string} traces_file
 * @property {number} [presignedUrlFileExpirationTime] in seconds
 */

/**
 * Object metadata returned by the listing operations
 *
 * @typedef BucketItem
 * @property {string} name name of the object (its key).
 * @property {string} [prefix] name of the object prefix.
 * @property {number} [size] size of the object.
 * @property {string} [etag] etag of the object.
 * @property {string} [versionId] versionId of the object.
 * @property {boolean} [isDeleteMarker] true if it is a delete marker.
 * @property {Date} [lastModified] modified time stamp.
 */

/**
 * @typedef FPutResult
 * @property {string} [etag] etag of the object.
 * @property {string} [versionId] versionId of the object.
 */

/**
 * Metadata of an object: the `Content-Type` key sets its content type, the rest
 * of the keys are stored as S3 user metadata (i.e. `x-amz-meta-*` headers)
 *
 * @typedef {Object.<string, string>} MetadataObject
 */

/**
 * S3 allows at most 1000 keys per DeleteObjects request
 */
const MAX_DELETE_OBJECTS = 1000;

/**
 * MinioClient - S3 client for MinIO / RustFS object storage operations
 *
 * Features:
 * - Command based calls through the AWS SDK v3
 * - Internal endpoint for the server side calls, public endpoint to sign the presigned urls
 * - Missing buckets are created on demand
 * - Proper error handling with initialization checks
 * - Type-safe operations with BucketItem types
 * - Efficient parallel object fetching
 */
export class MinioClient {

    /**
     * @param {MinioOpts} opts
     */
    constructor(opts) {
        try {
            this.#opts = opts;
            const credentials = {
                accessKeyId: opts.accessKey,
                secretAccessKey: opts.secretKey,
            };
            const region = opts.region ?? 'us-east-1';
            // Server side calls go through the internal endpoint, presigned urls are signed with the public one
            const endpoint = buildEndpoint(opts.host, opts.port, opts.useSSL);
            const publicEndpoint = buildEndpoint(opts.publicHost ?? opts.host, opts.publicPort ?? opts.port, opts.useSSL);
            logger.info({ endpoint, publicEndpoint, bucket: opts.bucket }, 'S3 OPTS');
            this.#s3Client = new S3Client({
                region,
                endpoint,
                credentials,
                // RustFS uses path-style URLs by default; virtual-host style requires RUSTFS_SERVER_DOMAINS
                forcePathStyle: opts.forcePathStyle !== false,
            });
            this.#initialized = true;
            logger.info('MinioClient initialized successfully');
        } catch (err) {
            logger.error({ err }, 'Failed to initialize MinioClient');
            this.#opts = opts;
            this.#s3Client = null;
            this.#presignClient = null;
            this.#initialized = false;
        }
    }

    /** @type {MinioOpts} */
    #opts;

    /** @type {S3Client | null} */
    #s3Client;

    /** @type {S3Client | null} */
    #presignClient;

    /** @type {boolean} */
    #initialized;

    /**
     * Check if client is properly initialized
     */
    get isInitialized() {
        return this.#initialized;
    }

    /**
     * Get the default bucket name
     */
    get defaultBucket() {
        return this.#opts.bucket;
    }

    /**
     * Ensure client is initialized and return it
     * @returns {S3Client}
     * @throws Error if client is not initialized
     */
    #client() {
        if (!this.#initialized || !this.#s3Client) {
            throw new Error('MinioClient is not initialized');
        }
        return this.#s3Client;
    }

    /**
     * Ensure client is initialized and return the client used to sign the presigned urls
     * @returns {S3Client}
     * @throws Error if client is not initialized
     */
    #presigner() {
        if (!this.#initialized || !this.#presignClient) {
            throw new Error('MinioClient is not initialized');
        }
        return this.#presignClient;
    }

    /**
     * Check if an error means that the object does not exist
     * @param {any} err - The error thrown by the S3 client
     */
    #isNotFound(err) {
        return err?.name === 'NoSuchKey' || err?.name === 'NotFound' || err?.$metadata?.httpStatusCode === 404;
    }

    /**
     * Check if an error means that the bucket does not exist
     * @param {any} err - The error thrown by the S3 client
     */
    #isNoSuchBucket(err) {
        return err?.name === 'NoSuchBucket' || (err?.$metadata?.httpStatusCode === 404 && err?.name !== 'NoSuchKey');
    }

    /**
     * Check if a bucket exists
     * @param {string} [bucket] - Bucket name, defaults to the configured one
     * @returns {Promise<boolean>} Promise resolving to true if the bucket exists
     */
    async bucketExists(bucket) {
        const client = this.#client();
        const name = bucket ?? this.#opts.bucket;
        try {
            await client.send(new HeadBucketCommand({ Bucket: name }));
            return true;
        } catch (err) {
            logger.debug({ bucket: name, err }, 'S3: bucket not found');
            return false;
        }
    }

    /**
     * Create a bucket if it does not exist yet
     * @param {string} [bucket] - Bucket name, defaults to the configured one
     * @returns {Promise<void>}
     */
    async ensureBucket(bucket) {
        const client = this.#client();
        const name = bucket ?? this.#opts.bucket;
        if (await this.bucketExists(name)) {
            return;
        }
        logger.info({ bucket: name }, 'S3: creating bucket');
        await client.send(new CreateBucketCommand({ Bucket: name }));
    }

    /**
     * Run an operation, creating the target bucket and retrying once if it is missing
     * @template T
     * @param {string} bucket - Bucket used by the operation
     * @param {() => Promise<T>} operation - Operation to run
     * @returns {Promise<T>}
     */
    async #withBucket(bucket, operation) {
        try {
            return await operation();
        } catch (err) {
            if (this.#isNoSuchBucket(err)) {
                logger.warn({ bucket }, 'S3: bucket missing, creating it and retrying');
                await this.ensureBucket(bucket);
                return await operation();
            }
            throw err;
        }
    }

    /**
     * List all the pages of objects with a prefix in a bucket
     * @param {string} bucket - Bucket name
     * @param {string} prefix - Object prefix filter
     * @returns {Promise<BucketItem[]>} Promise resolving to array of bucket items
     */
    async #listObjects(bucket, prefix) {
        const client = this.#client();
        logger.debug({ bucket, prefix }, 'S3: listObjects');
        /** @type {BucketItem[]} */
        const items = [];
        let continuationToken;
        do {
            const response = await this.#withBucket(bucket, () => client.send(new ListObjectsV2Command({
                Bucket: bucket,
                Prefix: prefix,
                ContinuationToken: continuationToken,
            })));
            for (const object of response?.Contents ?? []) {
                items.push({
                    name: /** @type {string} */ (object.Key),
                    prefix,
                    size: object.Size ?? 0,
                    etag: object.ETag,
                    lastModified: object.LastModified,
                });
            }
            continuationToken = response?.IsTruncated ? response?.NextContinuationToken : undefined;
        } while (continuationToken);
        return items;
    }

    /**
     * List objects of the default bucket with a prefix
     * @param {string} prefix - Object prefix filter
     * @returns {Promise<BucketItem[]>} Promise resolving to array of bucket items
     */
    async listFiles(prefix) {
        return await this.#listObjects(this.#opts.bucket, prefix);
    }

    /**
     * List objects in a bucket with a prefix
     * @param {string} bucket - Bucket name
     * @param {string} prefix - Object prefix filter
     * @returns {Promise<BucketItem[]>} Promise resolving to array of bucket items
     */
    async listMinioObjects(bucket, prefix) {
        return await this.#listObjects(bucket, prefix);
    }

    /**
     * Get the metadata of an object of the default bucket
     * @param {string} filePath - Path to the object
     * @returns {Promise<import("@aws-sdk/client-s3").HeadObjectCommandOutput>}
     * @throws {NotFoundError} if the object does not exist
     */
    async getMetadataObject(filePath) {
        const client = this.#client();
        logger.debug({ filePath }, 'S3: getMetadataObject');
        try {
            return await client.send(new HeadObjectCommand({
                Bucket: this.#opts.bucket,
                Key: filePath,
            }));
        } catch (err) {
            if (this.#isNotFound(err)) {
                throw new NotFoundError(`file ${filePath} not found`);
            }
            throw err;
        }
    }

    /**
     * Get file content from the default bucket
     * @param {string} file - Path to the file
     * @returns {Promise<string>} Promise resolving to file content as string
     * @throws {NotFoundError} if the file does not exist
     */
    async getFile(file) {
        const client = this.#client();
        const bucket = this.#opts.bucket;
        logger.debug({ file }, 'S3: getFile');
        try {
            return await this.#withBucket(bucket, async () => {
                const response = await client.send(new GetObjectCommand({
                    Bucket: bucket,
                    Key: file,
                }));
                if (!response?.Body) {
                    throw new NotFoundError(`file ${file} is empty`);
                }
                return await response.Body.transformToString('utf-8');
            });
        } catch (err) {
            if (err instanceof NotFoundError || this.#isNotFound(err)) {
                throw new NotFoundError(`file ${file} not found`);
            }
            logger.error({ err, file }, 'S3: getFile failed');
            throw err;
        }
    }

    /**
     * Get object content from a specific bucket
     * @param {string} bucket - Bucket name
     * @param {string} name - Object name/path
     * @returns {Promise<string>} Promise resolving to object content as string
     * @throws {NotFoundError} if the object does not exist
     */
    async getObject(bucket, name) {
        const client = this.#client();
        logger.debug({ bucket, name }, 'S3: getObject');
        try {
            const response = await this.#withBucket(bucket, () => client.send(new GetObjectCommand({
                Bucket: bucket,
                Key: name,
            })));
            if (!response?.Body) {
                throw new NotFoundError(`object ${name} is empty`);
            }
            return await response.Body.transformToString('utf-8');
        } catch (err) {
            if (err instanceof NotFoundError || this.#isNotFound(err)) {
                throw new NotFoundError(`object ${name} not found`);
            }
            throw err;
        }
    }

    /**
     * Get all objects with a prefix and return as JSON array string
     * @param {string} bucket - Bucket name
     * @param {string} prefix - Object prefix filter
     * @returns {Promise<string>} Promise resolving to JSON array string of all object contents
     */
    async getMinioObjects(bucket, prefix) {
        logger.debug({ bucket, prefix }, 'S3: getMinioObjects');
        const objectsList = await this.listMinioObjects(bucket, prefix);
        // Fetch contents in parallel
        const contents = await Promise.all(objectsList.map((obj) => this.getObject(bucket, obj.name)));
        return `[${contents.join(',')}]`;
    }

    /**
     * Get multiple files from the default bucket in parallel
     * @param {string[]} paths - Array of file paths
     * @returns {Promise<string[]>} Promise resolving to array of file contents
     */
    async getFiles(paths) {
        this.#client();
        logger.debug({ count: paths.length }, 'S3: getFiles');
        return Promise.all(paths.map((path) => this.getFile(path)));
    }

    /**
     * Get the traces of an activity
     * @param {string} activityId
     * @returns {Promise<BucketItem[]>}
     */
    async getTraces(activityId) {
        return this.listFiles(`${this.#opts.topics_dir}/${this.#opts.traces_topic}/_id=${activityId}/`);
    }

    /**
     * Download a file of the default bucket to the local filesystem
     * @param {string} remotePath - Path of the object
     * @param {string} localPath - Local destination path
     * @returns {Promise<void>}
     * @throws {NotFoundError} if the object does not exist
     */
    async copyFromRemoteFile(remotePath, localPath) {
        const client = this.#client();
        const bucket = this.#opts.bucket;
        logger.debug({ remotePath, localPath }, 'S3: copyFromRemoteFile');
        try {
            const response = await this.#withBucket(bucket, () => client.send(new GetObjectCommand({
                Bucket: bucket,
                Key: remotePath,
            })));
            if (!response?.Body) {
                throw new NotFoundError(`file ${remotePath} is empty`);
            }
            await ensureDirectoryStructureExists(dirname(localPath));
            // In the nodejs runtime the body of a GetObject response is a readable stream
            const body = /** @type {import('node:stream').Readable} */ (response.Body);
            await pipeline(body, createWriteStream(localPath));
        } catch (err) {
            if (this.#isNotFound(err)) {
                throw new NotFoundError(`file ${remotePath} not found`);
            }
            throw err;
        }
    }

    /**
     * Upload a local file to the default bucket
     * @param {string} localPath - Local source path
     * @param {string} remotePath - Path of the object
     * @param {MetadataObject} [metadata] - Content type and user metadata of the object
     * @returns {Promise<FPutResult>}
     */
    async copyToRemoteFile(localPath, remotePath, metadata) {
        logger.debug(`Copying file ${localPath} to remote ${remotePath}`);
        return await this.putObject(remotePath, createReadStream(localPath), metadata);
    }

    /**
     * Store a file in the default bucket
     * @param {string} file - Path to the file
     * @param {string} content - Content to store
     * @returns {Promise<FPutResult>}
     */
    async setFile(file, content) {
        return await this.putObject(file, content);
    }

    /**
     * Store a file in the default bucket
     * @param {string} file - Path to the file
     * @param {string} content - Content to store
     * @returns {Promise<void>}
     */
    async putFile(file, content) {
        await this.putObject(file, content);
    }

    /**
     * Upload an object into the default bucket
     * @param {string} file - Path to the object
     * @param {import("@smithy/types").StreamingBlobPayloadInputTypes} body - Content to store
     * @param {MetadataObject} [metadata] - Content type and user metadata of the object
     * @returns {Promise<FPutResult>}
     */
    async putObject(file, body, metadata) {
        const client = this.#client();
        const bucket = this.#opts.bucket;
        logger.debug({ file }, 'S3: putObject');
        const { contentType, userMetadata } = splitMetadata(metadata);
        const response = await this.#withBucket(bucket, () => client.send(new PutObjectCommand({
            Bucket: bucket,
            Key: file,
            Body: body,
            ContentType: contentType,
            Metadata: userMetadata,
        })));
        return { etag: response.ETag, versionId: response.VersionId };
    }

    /**
     * Remove a file from the default bucket
     * @param {string} path - Path to the file
     * @returns {Promise<void>}
     */
    async removeRemoteFile(path) {
        const client = this.#client();
        const bucket = this.#opts.bucket;
        logger.debug(`removeRemoteFile file ${path}`);
        await this.#withBucket(bucket, () => client.send(new DeleteObjectCommand({
            Bucket: bucket,
            Key: path,
        })));
    }

    /**
     * Remove several files from the default bucket
     * @param {(string | BucketItem)[]} paths - Paths of the files
     * @returns {Promise<void>}
     */
    async removeRemoteFiles(paths) {
        const client = this.#client();
        const bucket = this.#opts.bucket;
        logger.debug({ count: paths.length }, 'S3: removeRemoteFiles');
        const keys = paths.map((path) => typeof path === 'string' ? path : path.name);
        for (let idx = 0; idx < keys.length; idx += MAX_DELETE_OBJECTS) {
            const chunk = keys.slice(idx, idx + MAX_DELETE_OBJECTS);
            await this.#withBucket(bucket, () => client.send(new DeleteObjectsCommand({
                Bucket: bucket,
                Delete: {
                    Objects: chunk.map((Key) => ({ Key })),
                },
            })));
        }
    }

    /**
     * Remove a file from the default bucket
     * @param {string} file - Path to the file
     * @returns {Promise<void>}
     */
    async removeFile(file) {
        return await this.removeRemoteFile(file);
    }

    /**
     * Copy a file of the default bucket to another path, removing the original one
     * @param {string} oldPath - Current path of the file
     * @param {string} newPath - New path of the file
     * @returns {Promise<void>}
     */
    async renameFile(oldPath, newPath) {
        this.#client();
        if (await this.fileExists(oldPath)) {
            logger.debug({ oldPath, newPath }, 'S3: renameFile');
            const content = await this.getFile(oldPath);
            await this.putFile(newPath, content);
            await this.removeFile(oldPath);
        }
    }

    /**
     * Check if a file exists in the default bucket
     * @param {string} path - Path to check
     * @returns {Promise<boolean>} Promise resolving to true if file exists
     */
    async fileExists(path) {
        const client = this.#client();
        logger.debug({ path }, 'S3: fileExists');
        try {
            await client.send(new HeadObjectCommand({
                Bucket: this.#opts.bucket,
                Key: path,
            }));
            logger.debug({ path }, 'S3: file exists');
            return true;
        } catch (err) {
            if (this.#isNotFound(err) || this.#isNoSuchBucket(err)) {
                logger.debug({ path }, 'S3: file not found');
                return false;
            }
            logger.error({ err, path }, 'S3: fileExists failed');
            return false;
        }
    }

    /**
     * Check if multiple files exist
     * @param {string[]} paths - Array of file paths
     * @returns {Promise<Map<string, boolean>>} Promise resolving to map of path -> exists
     */
    async filesExist(paths) {
        this.#client();
        logger.debug({ count: paths.length }, 'S3: filesExist');
        const results = await Promise.all(paths.map(async (path) => ({ path, exists: await this.fileExists(path) })));
        return new Map(results.map((r) => [r.path, r.exists]));
    }

    /**
     * Copy an object of the default bucket to another path, without downloading it
     * @param {string} sourcePath - Current path of the file
     * @param {string} destinationPath - New path of the file
     * @returns {Promise<void>}
     */
    async copyWithinMinIO(sourcePath, destinationPath) {
        const client = this.#client();
        const bucket = this.#opts.bucket;
        logger.debug(`Copying file into remote from ${sourcePath} to ${destinationPath}`);
        await this.#withBucket(bucket, () => client.send(new CopyObjectCommand({
            Bucket: bucket,
            Key: destinationPath,
            CopySource: `${bucket}/${encodeKey(sourcePath)}`,
        })));
    }
}

/**
 * Build the S3 endpoint from the configured url and port
 * @param {string} url - Url of the object storage service, with or without protocol
 * @param {number} [port] - Port of the object storage service
 * @param {boolean} [ssl=true] - Whether to use https when the url has no protocol
 * @returns {string} The endpoint to be used by the S3 client
 */
function buildEndpoint(url, port, ssl = true) {
    const endpoint = new URL(url.match(/^https?:\/\//) ? url : `${ssl ? 'https' : 'http'}://${url}`);
    if (port && !endpoint.port && [80, 443].indexOf(port) === -1) {
        endpoint.port = `${port}`;
    }
    // Keep any path prefix (deployments served under a subpath), drop the root one
    return endpoint.pathname === '/' ? endpoint.origin : endpoint.href.replace(/\/+$/, '');
}

/**
 * Split the metadata of an object into its content type and its user metadata
 * @param {MetadataObject} [metadata]
 * @returns {{contentType?: string, userMetadata?: Record<string, string>}}
 */
function splitMetadata(metadata) {
    if (metadata === undefined) {
        return {};
    }
    /** @type {string | undefined} */
    let contentType;
    /** @type {Record<string, string>} */
    const userMetadata = {};
    for (const [key, value] of Object.entries(metadata)) {
        if (value === undefined || value === null) {
            continue;
        }
        if (key.toLowerCase() === 'content-type') {
            contentType = value;
        } else {
            // S3 user metadata keys are sent lowercase as x-amz-meta-* headers
            userMetadata[key.toLowerCase()] = value;
        }
    }
    return { contentType, userMetadata: Object.keys(userMetadata).length > 0 ? userMetadata : undefined };
}

/**
 * Encode an object key to be used as the source of a CopyObject request
 * @param {string} key
 * @returns {string}
 */
function encodeKey(key) {
    return key.split('/').map(encodeURIComponent).join('/');
}

// Singleton instance with default config
const minioClient = new MinioClient(config.minio);

export { minioClient };
export default MinioClient;
