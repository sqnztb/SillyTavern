import fs from 'node:fs';
import { Buffer } from 'node:buffer';
import { parentPort } from 'node:worker_threads';

// Each thread has its own fetch, and the image codecs need the patched one to load their WASM files
import './fetch-patch.js';
import { applyAvatarCropResize } from './avatar-image.js';
import { read, write } from './character-card-parser.js';
import { Jimp } from './jimp.js';

/**
 * Copies a buffer into memory of its own, so it can be transferred without copying it again.
 * Node buffers can share a pool that must not be detached.
 * @param {Uint8Array} buffer Buffer to copy
 * @returns {Uint8Array} Standalone copy
 */
const toTransferable = (buffer) => new Uint8Array(buffer);

/**
 * Reads the card data of an uploaded PNG and normalizes its image.
 * @param {string} filePath Path to the uploaded card
 * @returns {Promise<{ result: { cardData: string, image: Uint8Array }, transfer: ArrayBuffer[] }>}
 */
async function prepare(filePath) {
    const file = await fs.promises.readFile(filePath);
    const cardData = read(file);
    const image = toTransferable(await applyAvatarCropResize(await Jimp.fromBuffer(file)));
    return { result: { cardData, image }, transfer: /** @type {ArrayBuffer[]} */ ([image.buffer]) };
}

/**
 * Writes card data into a normalized PNG image.
 * @param {Uint8Array} image PNG image
 * @param {string} data Card data
 * @returns {{ result: Uint8Array, transfer: ArrayBuffer[] }}
 */
function compose(image, data) {
    const output = toTransferable(write(Buffer.from(image.buffer, image.byteOffset, image.byteLength), data));
    return { result: output, transfer: /** @type {ArrayBuffer[]} */ ([output.buffer]) };
}

parentPort.on('message', async (message) => {
    try {
        const { result, transfer } = message.type === 'prepare'
            ? await prepare(message.filePath)
            : compose(message.image, message.data);
        parentPort.postMessage({ result }, transfer);
    } catch (error) {
        parentPort.postMessage({ error: String(error?.message ?? error) });
    }
});
