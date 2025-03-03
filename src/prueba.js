import { join } from 'node:path';
import pino from 'pino';
import { listFiles as listFilesDirectory, withFile } from './utils/file.js';

export const logger = pino.pino({
    level: 'debug',
    serializers: {
        err: pino.stdSerializers.err,
        req: pino.stdSerializers.req,
        res: pino.stdSerializers.res
    },
    transport: {
        targets: [
            {
                target: 'pino-pretty',
                level: (process.env.LOG_LEVEL || 'info').toLowerCase(),
                options: {
                    translateTime: 'SYS:hh:MM:ss TT',
                    ignore: 'pid,hostname'
                }
            }
        ]
    }
});

/*
let err1 = new Error('err1')
console.error(err1)
logger.error(err1)
logger.error({ err1 })
*/
try {
    const activityPath = join('state', '01X.json');
    const files = await withFile(activityPath);
    const content = await files(async (file) => {
        let content = '';
        for await (const line of file.readLines()) {
            content += line;
        }
        return content;
    }, false);
    console.log(`Files: ${content}`)
} catch(e) {
    console.log('console.error');
    console.error(e);
    logger.error('plain')
    logger.error(e);
    logger.error('Nested')
    logger.error(new Error('Oops', {cause: e}));
}