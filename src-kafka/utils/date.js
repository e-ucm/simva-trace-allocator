import * as Duration from 'tinyduration';

/**
 * 
 * @returns 
 */
export function now() {
    return new Date();
}

/**
 * 
 * @returns 
 */
export function epoch() {
    return new Date(0);
}

/**
 * 
 * @param {string} strDate 
 * @returns 
 */
export function parseDate(strDate) {
    const millis = Date.parse(strDate);
    return new Date(millis);
}

/**
 * 
 * @param {Date} start 
 * @param {Date} end 
 * 
 * @returns {number}
 */
export function duration(start, end) {
    const endMillis = end.getTime();
    const startMillis = start.getTime();
    return endMillis - startMillis +1;
}

const MILLIS_TO_SECONDS = 1000;
const MILLIS_TO_MINUTES = 60 * MILLIS_TO_SECONDS;
const MILLIS_TO_HOURS = 60 * MILLIS_TO_MINUTES;
const MILLIS_TO_DAYS = 24 * MILLIS_TO_HOURS;
const MILLIS_TO_MONTHS = 30 * MILLIS_TO_DAYS;
const MILLIS_TO_YEARS = 12 * MILLIS_TO_MONTHS;

/**
 * 
 * @param {number} durationMillis 
 * @returns {string}
 */
export function formatDuration(durationMillis) {
    const duration = {};

    let value = Math.floor(durationMillis / MILLIS_TO_YEARS);
    durationMillis = durationMillis % MILLIS_TO_YEARS;
    duration.years = value;

    value = Math.floor(durationMillis / MILLIS_TO_MONTHS);
    durationMillis = durationMillis % MILLIS_TO_MONTHS;
    duration.months = value;

    value = Math.floor(durationMillis / MILLIS_TO_DAYS);
    durationMillis = durationMillis % MILLIS_TO_DAYS;
    duration.days = value;

    value = Math.floor(durationMillis / MILLIS_TO_HOURS);
    durationMillis = durationMillis % MILLIS_TO_HOURS;
    duration.hours = value;

    value = Math.floor(durationMillis / MILLIS_TO_MINUTES);
    durationMillis = durationMillis % MILLIS_TO_MINUTES;
    duration.minutes = value;

    value = Math.floor(durationMillis / MILLIS_TO_SECONDS);
    durationMillis = durationMillis % MILLIS_TO_SECONDS;
    duration.seconds = value;

    return Duration.serialize(duration);
}