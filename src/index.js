import { config, isInDebugMode } from './config.js';
import { logger } from './logger.js';

import { Compactor } from './compactor.js';
import { getState } from './state.js';

logger.debug('Current config: %o', config);

const compactor = new Compactor(config);
const run = compactor.compact.bind(compactor);

let state = await getState(compactor.getOpts(), compactor.getMinioClient());

if(state.size === 0) {
    try {
        // Run the compact function immediately at launch
        logger.info('Running compactor at launch...');
        await run();
        logger.info('Compactor initialized.');
    } catch (error) {
        logger.error('Error during compactor initialization:', error);
    }
}

// Start consuming messages
(async () => {
    await compactor.startKafkaConsumer();
})();