import { config, isInDebugMode } from './config.js';
import { logger } from './logger.js';

import { Compactor } from './compactor.js';

const compactor = new Compactor(config);
const run = compactor.compact.bind(compactor);

const MAX_WAIT_TIME_ON_EXIT = 30*1000;

logger.debug('Current config: %o', config);

let intervalId;

async function init() {
    try {
        // Run the compact function immediately at launch
        logger.info('Running compactor at launch...');
        await run();

        // Set an interval to run compact periodically if not in debug mode
        if (!isInDebugMode()) {
            intervalId = setInterval(run, config.refreshInterval);
        }

        logger.info('Compactor initialized.');
    } catch (error) {
        logger.error('Error during compactor initialization:', error);
    }
}

// Initialize the compactor at launch
init();

process.on('SIGTERM', () => {
    logger.info('SIGTERM signal received: terminating');
    if (intervalId !== undefined) {
        clearInterval(intervalId);
    }
    compactor.shouldExit = true;
});

process.on('SIGINT', () => {
    logger.info('SIGINT signal received');
    if (intervalId !== undefined) {
        clearInterval(intervalId);
    }
    compactor.shouldExit = true;
    setTimeout(() => {
        logger.info('Exiting');
        process.exit();        
    }, MAX_WAIT_TIME_ON_EXIT);
});


process.on('SIGUSR2',function(){
    logger.info("SIGUSR2 signal received");
    if (!compactor.status.processing) {
        setImmediate(run);
        logger.info(`Force run`);
    }
    const status = compactor.status;
    const elapsedTime = compactor.elapsedTime;
    logger.info(`Status: ${status.current} / ${status.total}, elapsedTime: ${elapsedTime}`);
});