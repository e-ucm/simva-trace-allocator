import 'dotenv/config';
import * as inspector from 'inspector';
import ms from "ms";

/**
 * @typedef CompactorOptions
 * @property {boolean} concatEventPolicy 
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
 * @property {import('./kafka.js').KafkaOpts} kafka
 */

const rustfs_enabled = process.env.RUSTFS_ENABLED !== 'false';
const rustfs_api_url = process.env.RUSTFS_API_URL || 'rustfs-api.external.test';
const rustfs_ssl = process.env.RUSTFS_SSL !== 'false';
const rustfs_port = parseInt(`${process.env.RUSTFS_PORT}`) || 443;
const rustfs_access_key = process.env.RUSTFS_ACCESS_KEY || 'rustfs';
const rustfs_secret_key = process.env.RUSTFS_SECRET_KEY || 'secret';
const minio_apiUrl = process.env.MINIO_API_URL || 'minio-api.external.test';
const minio_ssl = process.env.MINIO_SSL !== 'false';
const minio_port = parseInt(`${process.env.MINIO_PORT}`) || 443;
const minio_accessKey = process.env.MINIO_ACCESS_KEY || 'minio';
const minio_secretKey = process.env.MINIO_SECRET_KEY || 'secret';

/** @type {CompactorOptions} */
export const config = {
    concatEventPolicy: process.env.CONCAT_EVENT_POLICY !== undefined ? (process.env.CONCAT_EVENT_POLICY.toLocaleLowerCase() === 'false' ? false : true) : true, // if true minio-events else previous version of trace allocator
    batchSize: process.env.BATCH_SIZE !== undefined ? parseInt(process.env.BATCH_SIZE) : 500,
    maxDelay: process.env.MAX_DELAY !== undefined ? ms(process.env.MAX_DELAY) : ms("5min"),
    refreshInterval: process.env.REFRESH_INTERVAL !== undefined ? ms(process.env.REFRESH_INTERVAL) : ms("10min"),
    localStatePath: process.env.LOCAL_STATE || new URL('../state', import.meta.url).pathname,
    remoteStatePath: process.env.REMOTE_STATE || 'state',
    removeDryRun: process.env.REMOVE_DRY_RUN !== undefined ? (process.env.REMOVE_DRY_RUN.toLocaleLowerCase() === 'false' ? false : true) : true,
    gcInterval: process.env.GC_INTERVAL !== undefined ? ms(process.env.GC_INTERVAL) : ms("2h"),
    copyInsteadRename: process.env.COPY_INSTEAD_RENAME !== undefined ? (process.env.COPY_INSTEAD_RENAME.toLocaleLowerCase() === 'false' ? false : true) : true,
    tryRecovery: process.env.TRY_RECOVERY !== undefined ? (process.env.TRY_RECOVERY.toLocaleLowerCase() === 'false' ? false : true) : false,
    minio: {
        // Internal (in network) endpoint used by the server to talk to the object storage service.
        // Inside the container network the external hostnames do not resolve, so the service is
        // reached through its internal service name (i.e. rustfs.internal.test).
        host: rustfs_enabled ? rustfs_api_url : minio_apiUrl,
        useSSL: rustfs_enabled ? rustfs_ssl : minio_ssl,
        port: rustfs_enabled ? rustfs_port : minio_port,
        region: process.env.RUSTFS_REGION || process.env.AWS_REGION || 'us-east-1',
        accessKey: rustfs_enabled ? rustfs_access_key : minio_accessKey,
        secretKey: rustfs_enabled ? rustfs_secret_key : minio_secretKey,
        bucket: process.env.MINIO_BUCKET || 'traces',
        topics_dir: process.env.MINIO_TOPICS_DIR || 'kafka-topics',
        traces_topic: process.env.MINIO_TRACES_TOPIC || 'traces',
        outputs_dir: process.env.MINIO_OUTPUTS_DIR || 'outputs',
        traces_file: process.env.MINIO_TRACES_FILE || 'traces_v2.json',
    },
    simva: {
        host: process.env.SIMVA_HOST || 'simva-api.simva.example.org',
        protocol: process.env.SIMVA_PROTOCOL || 'https',
        port: process.env.SIMVA_PORT !== undefined ? parseInt(process.env.SIMVA_PORT) : undefined,
        username: process.env.SIMVA_USER || 'admin',
        password: process.env.SIMVA_PASSWORD || 'password',
        ssoHost: process.env.SIMVA_SSO_HOST || 'sso.simva.example.org',
        ssoProtocol: process.env.SIMVA_SSO_PROTOCOL || 'https',
        ssoPort: process.env.SIMVA_SSO_PORT !== undefined ? parseInt(process.env.SIMVA_SSO_PORT) : undefined,
        ssoRealm: process.env.SIMVA_SSO_REALM || 'simva',
        clientId: process.env.SIMVA_CLIENT_ID || 'simva-trace-allocator',
        clientSecret: process.env.SIMVA_CLIENT_SECRET || 'secret',
    },
    kafka: {
        clientId: process.env.SIMVA_KAFKA_CLIENTID || 'my-client-id',
        brokers: process.env.SIMVA_KAFKA_BROKER !== undefined ?  [ process.env.SIMVA_KAFKA_BROKER ] : ['localhost:9092'],
        groupId: process.env.SIMVA_KAFKA_GROUPID || 'my-group-id',
        topic: process.env.SIMVA_KAFKA_MINIO_TOPIC || 'minio-events'
    }
};

export function isInDebugMode() {
    return inspector.url() !== undefined;
}