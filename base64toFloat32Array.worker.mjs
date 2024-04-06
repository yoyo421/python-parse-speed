/**
 * @file base64toFloat32Array.worker.js
 */

import { workerInstance } from "./worker.base.mjs";

/**
 * @type {WorkerGlobalScope}
 */
const self = globalThis;

/**
 * @param {string} base64
 */
export function base64toFloat32Array(base64) {
  const binary = atob(base64);
  const bytes = new Uint8Array(binary.length);
  for (let i = 0; i < bytes.length; i++) {
    // the endianness is little-endian
    bytes[i] = binary.charCodeAt(i);
  }
  return new Float32Array(bytes.buffer);
}
const workerPools = workerInstance(
  {
    base64: base64toFloat32Array,
  },
  {
    filename: import.meta.url,
  }
);

/**
 * Ideally, we should use a pool of workers to process the data
 * @param {string} base64
 */
export function base64toFloat32ArrayWorker(base64) {
  return workerPools.runFromWorker("base64", base64);
}
