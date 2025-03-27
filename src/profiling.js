import path from 'path';
import { logger } from './logger.js';
import { fileURLToPath } from 'url';
import v8 from 'v8';
import process from 'node:process';
import ms from 'ms';

if(process.env.NODE_ENV == "development" && process.env.ENABLE_DEBUG_PROFILING == "true") {
  logger.info("Profiling in progress...");
}