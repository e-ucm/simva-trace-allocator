import { config } from './config.js';
import { mkdir } from 'node:fs/promises';
import pino from 'pino';
import { now } from './dateUtils.js';

const logsFolder = new URL('../logs', import.meta.url);
const dirCreation = await mkdir(logsFolder, { recursive: true });
const logFile = `${logsFolder.pathname}/${now().toISOString()}.log`;

let transport = {
    targets: [
        {
            target: 'pino/file',
            level: (process.env.LOG_LEVEL || 'info').toLowerCase(),
            options: {
                destination: logFile
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
                    translateTime: 'SYS:hh:MM:ss TT',
                    singleLine: true,
                    ignore: 'pid,hostname'
                }
            }
    );
} else {
    transport.targets.push(
        {
            target: 'pino/file',
            level: (process.env.LOG_LEVEL || 'info').toLowerCase(),
            options: {
                translateTime: 'SYS:hh:MM:ss TT',
                singleLine: true,
                ignore: 'pid,hostname'
            }
        }
);   
}

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
    }
}


export const logger = pino.pino(options);


process.on('uncaughtException', err => {
    logger.fatal(err, 'uncaughtException')
    process.exitCode = 1
});

process.on('unhandledRejection', reason =>
    logger.fatal(reason, 'unhandledRejection')
);
