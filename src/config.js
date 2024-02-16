import 'dotenv/config';
import * as inspector from 'inspector';

/**
 * @typedef CompactorOptions
 * @property {number} batchSize
 * @property {number} maxDelay
 * @property {number} refreshInterval
 * @property {string} localStatePath
 * @property {string} remoteStatePath
 * @property {boolean} removeDryRun
 * @property {number} gcInterval
 * @property {boolean} copyInsteadRename
 * @property {boolean} tryRecovery
 * @property {import('./minio.js').MinioOpts} minio
 * @property {import('./simva.js').SimvaOpts} simva
 */

/** @type {CompactorOptions} */
export const config = {
    batchSize: process.env.BATCH_SIZE !== undefined ? parseInt(process.env.BATCH_SIZE) : 500,
    maxDelay: process.env.MAX_DELAY !== undefined ? parseInt(process.env.MAX_DELAY) : 5*60*1000,
    refreshInterval: process.env.REFRESH_INTERVAL !== undefined ? parseInt(process.env.REFRESH_INTERVAL) : 10*60*1000,
    localStatePath: process.env.LOCAL_STATE || new URL('../state', import.meta.url).pathname,
    remoteStatePath: process.env.REMOTE_STATE || 'state',
    removeDryRun: process.env.REMOVE_DRY_RUN !== undefined ? (process.env.REMOVE_DRY_RUN.toLocaleLowerCase() === 'false' ? false : true) : true,
    gcInterval: process.env.GC_INTERVAL !== undefined ? parseInt(process.env.GC_INTERVAL) : 10*24*60*60*1000,
    copyInsteadRename: process.env.COPY_INSTEAD_RENAME !== undefined ? (process.env.COPY_INSTEAD_RENAME.toLocaleLowerCase() === 'false' ? false : true) : true,
    tryRecovery: process.env.TRY_RECOVERY !== undefined ? (process.env.TRY_RECOVERY.toLocaleLowerCase() === 'false' ? false : true) : false,
    minio: {
        host: process.env.MINIO_HOST || 'minio.simva.example.org',
        useSSL: process.env.MINIO_SSL !== undefined ? (process.env.MINIO_SSL.toLocaleLowerCase() === 'false' ? false : true) : false,
        port: process.env.MINIO_PORT !== undefined ? parseInt(process.env.MINIO_PORT) : undefined,
        accessKey: process.env.MINIO_ACCESS_KEY || 'admin',
        secretKey: process.env.MINIO_SECRET_KEY || 'ChanGeMe',
        bucket: process.env.MINIO_BUCKET || 'traces',
        topics_dir: process.env.MINIO_TOPICS_DIR || 'kafka-topics',
        traces_topic: process.env.MINIO_TRACES_TOPIC || 'traces',
        users_dir: process.env.MINIO_USERS_DIR || 'users',
        traces_file: process.env.MINIO_TRACES_FILE || 'traces_v2.json',
    },
    simva: {
        host: process.env.SIMVA_HOST || 'simva-api.simva.example.org',
        protocol: process.env.SIMVA_PROTOCOL || 'https',
        port: process.env.SIMVA_PORT !== undefined ? parseInt(process.env.SIMVA_PORT) : undefined,
        username: process.env.SIMVA_USER || 'admin',
        password: process.env.SIMVA_PASSWORD || 'password',
    }
};

export function isInDebugMode() {
    return inspector.url() !== undefined;
}
