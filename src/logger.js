import { config } from './config.js';
import pino from 'pino';
import { now } from './utils/date.js';

const logsFolder = new URL('../logs', import.meta.url);
const logFile = `${logsFolder.pathname}/${now().toISOString()}.log`;

/** @type {{targets:import('pino').TransportTargetOptions[]}} */
let transport = {
    targets: [
        {
            target: 'pino/file',
            level: (process.env.LOG_LEVEL || 'info').toLowerCase(),
            options: {
                destination: logFile,
                mkdir: true
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
} else {
    transport.targets.push(
        {
            target: 'pino/file',
            level: (process.env.LOG_LEVEL || 'info').toLowerCase(),
            options: {
                singleLine: true,
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
