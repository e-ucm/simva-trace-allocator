import path from 'path';
import { config } from './config.js';
import { logger } from './logger.js';
import { fileURLToPath } from 'url';
import cron from 'node-cron';
import { convertTimeToCron } from "./utils/date.js";
import v8 from 'v8';
import process from 'node:process';
import ms from 'ms';

if(process.env.NODE_ENV == "development" && config.enabled_debug_profiling) {
  logger.info("Profiling in progress...");
  const __filename = fileURLToPath(import.meta.url);
  const __dirname = path.dirname(__filename);
  const profilingFolder =process.env.PROFILING_FOLDER || path.join(__dirname, '../../profiling');

  function heapdump() {
    logger.info(`schedule task for profiling running...`);
    //let filename=`${profilingFolder}/Heap.${now().toISOString()}.heapsnapshot`;
    let filename=`${profilingFolder}/${v8.writeHeapSnapshot()}`;
    logger.info(`Saved heapdump into ${v8.writeHeapSnapshot(filename)}`);
    setTimeout(heapdump, ms("30min"));
  }
  setTimeout(heapdump, ms("1min"));

  function memoryAndCPUUsage() {
    let memoryUsage=process.memoryUsage();
    let cpuUsage=process.cpuUsage();
    logger.info(`MEMORY USAGE : ${JSON.stringify(memoryUsage)}`);
    logger.info(`CPU USAGE : ${JSON.stringify(cpuUsage)}`);
    const heapUsed = memoryUsage.heapUsed / 1024 / 1024;
    const arrayBuffers = memoryUsage.arrayBuffers / 1024 / 1024;
    const rss = memoryUsage.rss / 1024 / 1024;
    const external = memoryUsage.external / 1024 / 1024;
    const heapTotal = memoryUsage.heapTotal / 1024 / 1024;
    logger.info(`MEMORY USAGE : This app is currently using ${Math.floor(heapUsed)} MB of memory (heapTotal : ${Math.floor(heapTotal)} MB - rss : ${Math.floor(rss)} MB - arrayBuffers : ${Math.floor(arrayBuffers)} MB -external : ${Math.floor(external)} MB).`);
    const user = cpuUsage.user / 1000;
    const system = cpuUsage.system / 1000;
    logger.info(`CPU USAGE : This app is currently using user ${Math.floor(user)}s and system ${Math.floor(system)}s`);
    setTimeout(memoryAndCPUUsage, ms("1min"));
  }
  setTimeout(memoryAndCPUUsage, ms("1min"));
}

/*
import {
  Worker,
  isMainThread,
  parentPort,
} from 'node:worker_threads';

if (isMainThread) {
  const worker = new Worker(__filename);

  worker.once('message', (filename) => {
    logger.info(`worker heapdump: ${filename}`);
    // Now get a heapdump for the main thread.
    logger.info(`main thread heapdump: ${v8.writeHeapSnapshot(`${profilingFolder}/Heap.${now().toISOString()}.heapsnapshot`)}`);
  });

  // Tell the worker to create a heapdump.
  worker.postMessage('heapdump');
} else {
  parentPort.once('message', (message) => {
    if (message === 'heapdump') {
      // Generate a heapdump for the worker
      // and return the filename to the parent.
      parentPort.postMessage(v8.writeHeapSnapshot(`${profilingFolder}/Heap.${now().toISOString()}.heapsnapshot`));
    }
  });
}
*/