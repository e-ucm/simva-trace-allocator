import { config, isInDebugMode } from './config.js';
import { logger } from './logger.js';

import { Compactor } from './compactor.js';
import { KafkaCompactor } from './kafkaCompactor.js';
import { getState } from './state.js';

logger.debug('Current config: %o', config);

const compactor = new Compactor(config);
const run = compactor.compact.bind(compactor);

const kafkaCompactor = new KafkaCompactor(config);
let state = await getState(kafkaCompactor.getOpts(), kafkaCompactor.getMinioClient());

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
    await kafkaCompactor.start();
})();