import os from 'node:os';
import { Buffer } from 'node:buffer';
import { Worker } from 'node:worker_threads';

/** Browsers open up to six connections per host, so more workers would sit idle. */
const MAX_WORKERS = 6;
/** Workers hold memory, so they are stopped shortly after a bulk import ends. */
const IDLE_SHUTDOWN_MS = 30_000;
/** Workers failing this many times in a row before finishing a job turn the pool off. */
const MAX_STARTUP_FAILURES = 2;
const WORKER_URL = new URL('./card-import-worker.js', import.meta.url);

// Leave one core for the server itself. Some Android devices require tighter memory management.
const poolSize = process.platform === 'android' ? 0 : Math.min(MAX_WORKERS, os.availableParallelism() - 1);

/**
 * @typedef {object} PoolJob
 * @property {object} message Message for the worker
 * @property {(value: any) => void} resolve Resolves the job
 * @property {(reason: Error) => void} reject Rejects the job
 */

/** @type {Set<Worker>} */
const workers = new Set();
/** @type {Worker[]} */
const idleWorkers = [];
/** @type {Map<Worker, PoolJob>} */
const activeJobs = new Map();
/** @type {PoolJob[]} */
const queuedJobs = [];
/** @type {NodeJS.Timeout|null} */
let shutdownTimer = null;
let startupFailures = 0;
let disabled = poolSize < 1;

/**
 * Stops all workers once the pool has been idle for a while.
 */
function scheduleShutdown() {
    if (shutdownTimer) clearTimeout(shutdownTimer);
    shutdownTimer = null;
    if (activeJobs.size > 0 || queuedJobs.length > 0 || workers.size === 0) return;

    shutdownTimer = setTimeout(() => {
        shutdownTimer = null;
        for (const worker of [...workers]) {
            workers.delete(worker);
            worker.terminate();
        }
        idleWorkers.length = 0;
    }, IDLE_SHUTDOWN_MS);
    shutdownTimer.unref();
}

/**
 * Handles a worker that stopped unexpectedly. Its job fails, and the caller falls back to the main thread.
 * @param {Worker} worker The worker
 * @param {boolean} finishedAJob Whether the worker ever finished a job
 * @param {Error} error Why it stopped
 */
function onWorkerLost(worker, finishedAJob, error) {
    if (!workers.delete(worker)) return;

    const idleIndex = idleWorkers.indexOf(worker);
    if (idleIndex >= 0) idleWorkers.splice(idleIndex, 1);

    const job = activeJobs.get(worker);
    activeJobs.delete(worker);
    job?.reject(error);

    if (!finishedAJob && ++startupFailures >= MAX_STARTUP_FAILURES && !disabled) {
        console.warn('Card import workers are unavailable. Using the main thread instead.', error);
        disabled = true;
    }

    dispatch();
}

/**
 * Starts a worker.
 * @returns {Worker} The new worker
 */
function startWorker() {
    const worker = new Worker(WORKER_URL);
    let finishedAJob = false;
    worker.unref();
    workers.add(worker);

    worker.on('message', ({ result, error }) => {
        const job = activeJobs.get(worker);
        activeJobs.delete(worker);
        finishedAJob = true;
        startupFailures = 0;
        idleWorkers.push(worker);

        if (error) job?.reject(new Error(error));
        else job?.resolve(result);
        dispatch();
    });
    worker.once('error', (error) => onWorkerLost(worker, finishedAJob, error));
    worker.once('exit', (code) => onWorkerLost(worker, finishedAJob, new Error(`Card import worker exited with code ${code}`)));
    return worker;
}

/**
 * Hands queued jobs to available workers.
 */
function dispatch() {
    while (queuedJobs.length > 0) {
        let worker = idleWorkers.pop();
        if (!worker && !disabled && workers.size < poolSize) {
            try {
                worker = startWorker();
            } catch (error) {
                console.warn('Could not start a card import worker. Using the main thread instead.', error);
                disabled = true;
            }
        }
        if (!worker) break;

        const job = queuedJobs.shift();
        activeJobs.set(worker, job);
        worker.postMessage(job.message);
    }

    // With no worker left to take them, queued jobs fall back to the main thread
    if (disabled && workers.size === 0) {
        for (const job of queuedJobs.splice(0)) {
            job.reject(new Error('Card import workers are unavailable'));
        }
    }

    scheduleShutdown();
}

/**
 * Runs a job on a worker.
 * @param {object} message Message for the worker
 * @returns {Promise<any>} The job result
 */
function runJob(message) {
    return new Promise((resolve, reject) => {
        queuedJobs.push({ message, resolve, reject });
        dispatch();
    });
}

/**
 * Reads the card data of an uploaded PNG and normalizes its image on another core.
 * @param {string} filePath Path to the uploaded card
 * @returns {Promise<{ cardData: string, image: Buffer }|null>} The result, or null if the main thread should do this work
 */
export async function prepareCardImport(filePath) {
    if (disabled) return null;

    try {
        const { cardData, image } = await runJob({ type: 'prepare', filePath });
        return { cardData, image: Buffer.from(image.buffer, image.byteOffset, image.byteLength) };
    } catch (error) {
        console.debug('Card import preparation fell back to the main thread:', error);
        return null;
    }
}

/**
 * Writes card data into a normalized PNG image on another core.
 * @param {Buffer} image Normalized PNG image. It is copied, so the caller can still use it after a failure.
 * @param {string} data Card data
 * @returns {Promise<Buffer|null>} The finished card, or null if the main thread should do this work
 */
export async function composeCardImage(image, data) {
    if (disabled) return null;

    try {
        const output = await runJob({ type: 'compose', image, data });
        return Buffer.from(output.buffer, output.byteOffset, output.byteLength);
    } catch (error) {
        console.debug('Card composition fell back to the main thread:', error);
        return null;
    }
}
