import { config } from './config.js';
import pino from 'pino';
import path, { join } from 'path';
import { now } from './utils/date.js';
import { fileURLToPath } from 'url';

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);
const logsFolder =process.env.LOG_FOLDER || path.join(__dirname, '../logs');
const logFile = `${logsFolder}/${now().toISOString()}.log`;

/** @type {{targets:import('pino').TransportTargetOptions[]}} */
let transport = {
    targets: [
        {
            target: 'pino/file',
            level: (process.env.LOG_LEVEL || 'info').toLowerCase(),
            options: {
                destination: logFile,
                singleLine: true
            }
        }
    ]
};
if (process.env.NODE_ENV !== 'production') {
    transport.targets.push(
        {
            target: 'pino-pretty',
            level: (process.env.LOG_LEVEL || 'info').toLowerCase(),
            options: {
                ignore: 'pid,hostname'
            }
        }
    );
}

/** @type {import('pino').LoggerOptions} */
const options = {
    level: (process.env.LOG_LEVEL || 'info').toLowerCase(),
    redact: {
        paths: ['minio.secretKey', 'simva.password'],
        censor: '**REDACTED**'
    },
    customLevels: { log: 30 },
    serializers: {
        err: pino.stdSerializers.err,
        req: pino.stdSerializers.req,
        res: pino.stdSerializers.res
    },
    transport
}


export const logger = pino.pino(options);


process.on('uncaughtException', err => {
    logger.fatal(err, 'uncaughtException')
    process.exitCode = 1
});

process.on('unhandledRejection', reason =>
    logger.fatal(reason, 'unhandledRejection')
);
