import { MinioClient } from './minio.js';
import { config } from './config.js';
import { logger } from './logger.js';
import ms from 'ms';
let minio = new MinioClient(config.minio);
const outputFilePath = "outputs/67b7679e9289f002b94083e9/traces.json";
const copyLittleDataFilePath = "/data/67b7515834931102b914d4ce/943b4a7d1e96c132e6ee4577076340f230477e82-files.txt";
//const copyDataFilePath = "/data/67b7679e9289f002b94083e9/02430353fc6b1c5b25d5852aab8c4e43a03de538-state.txt"; 
const copyDataFilePath = "/data/67b7679e9289f002b94083e9/traces.txt";

let metadata = {
    "Content-Type": "application/json",
    //"X-Amz-Meta-Version": "1.0"
};
async function copyToRemoteLittle() {
    try {
        logger.info(await minio.getMetadataObject(outputFilePath));
        await minio.copyToRemoteFile(copyLittleDataFilePath,outputFilePath, metadata);
        logger.info("Copy OK");
    } catch(e) {
        logger.error(e);
    }
    setTimeout(copyToRemote, ms("30s"));
}

async function copyToRemote() {
    try {
        logger.info(await minio.getMetadataObject(outputFilePath));
        await minio.copyToRemoteFile(copyDataFilePath,outputFilePath, metadata);
        logger.info("Copy OK");
    } catch(e) {
        logger.error(e);
    }
    setTimeout(copyToRemoteLittle, ms("30s"));
}

copyToRemoteLittle()