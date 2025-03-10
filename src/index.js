import { config, isInDebugMode } from './config.js';
import { logger } from './logger.js';
import cron from 'node-cron';
import { Compactor } from './compactor.js';
import { convertTimeToCron } from "./utils/date.js";
import { KafkaClient } from './kafka.js';
import "./profiling.js";
import ms from 'ms';
logger.debug('Current config: %o', config);

const compactor = new Compactor(config);
const run = compactor.compact.bind(compactor);

let intervalId;

if (config.concatEventPolicy) {
    const kafka = new KafkaClient(config.kafka);
    await startKafkaProcess();
    async function startKafkaProcess() {
        // Schedule a task to run every x
        let gcIntervalInMin=Math.round(config.gcInterval/(1000*60));
        const cronTime = convertTimeToCron(gcIntervalInMin);
        logger.info(cronTime);
        const task = async () => {
            logger.info("Compactor starting process garbage scheduled task running every x minutes");
            try {
                await compactor.processConsistencyAndGarbage();
            } catch(e) {
                logger.error(e);
            }
        };
        
        await task();
        
        cron.schedule(cronTime, task);
        
        try {
            logger.info('Compactor starting Kafka consumption...');
            // Start Kafka consumption and pass the processMessage as a callback
            await kafka.startKafkaConsumer(compactor.processMessage);
        } catch (error) {
            logger.error('Error starting Compactor:', error);
        }
    }
    // Method to stop consuming messages
    async function stopKafkaConsumer() {
        try {
            await kafka.disconnect();
            logger.info('Compactor stopped Kafka consumption.');
        } catch (error) {
            logger.error('Error stopping Compactor:', error);
        }
    }
} else {    
    const MAX_WAIT_TIME_ON_EXIT = ms("30 sec");
    await startPrevVersionProcess();

    async function startPrevVersionProcess() {
        try {
            // Run the compact function immediately at launch
            logger.info('Running compactor at launch...');
            await run();
    
            // Set an interval to run compact periodically if not in debug mode
            if (!isInDebugMode()) {
                intervalId = setInterval(run, config.refreshInterval);
            } else {
                await run();
            }
    
            logger.info('Compactor initialized.');
        } catch (error) {
            logger.error('Error during compactor initialization:', error);
        }
        
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
    }
}
