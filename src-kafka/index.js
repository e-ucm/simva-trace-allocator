import { config, isInDebugMode } from './config.js';
import { logger } from './logger.js';

import { Compactor } from './compactor.js';

const compactor = new Compactor(config);

logger.debug('Current config: %o', config);

// Start consuming messages
(async () => {
    await compactor.start();
})();


process.on('SIGTERM', () => {
    console.log('Received SIGTERM. Gracefully shutting down...');
    (async () => {
        await compactor.stop();
    })();
    process.exit(0);
  });

  process.on('SIGUSR2', () => {
    console.log('Received SIGUSR2. Performing custom action...');
    (async () => {
        await compactor.stop();
    })();
    logger.debug('Current config: %o', config);
    (async () => {
        await compactor.start();
    })();
  });

  process.on('SIGINT', () => {
    console.log('Received SIGINT. Gracefully shutting down...');
    (async () => {
        await compactor.stop();
    })();
    process.exit(0);  // Exit with a success status code
  });
  

