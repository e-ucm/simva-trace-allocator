import { MinioClient } from './minio.js';
import { config } from './config.js';
import { logger } from './logger.js';
import ms from 'ms';
let minio = new MinioClient(config.minio);

async function copyToRemoteLittle() {
    try {
        await minio.copyToRemoteFile("/data/67b7515834931102b914d4ce/943b4a7d1e96c132e6ee4577076340f230477e82-files.txt", "outputs/67b7679e9289f002b94083e9/traces.json");
        logger.info("Copy OK");
    } catch(e) {
        logger.error(e);
    }
    setTimeout(copyToRemote, ms("30s"));
}

async function copyToRemote() {
    try {
        await minio.copyToRemoteFile("/data/67b7679e9289f002b94083e9/traces.json", "outputs/67b7679e9289f002b94083e9/traces.json");
        logger.info("Copy OK");
    } catch(e) {
        logger.error(e);
    }
    setTimeout(copyToRemoteLittle, ms("30s"));
}

copyToRemoteLittle()