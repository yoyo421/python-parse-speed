/**
 * @file base64toFloat32Array.worker.js
 */

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
const workerPool = [];

if (globalThis["WorkerGlobalScope"] && self instanceof WorkerGlobalScope) {
  self.onmessage = function (event) {
    const base64 = event.data;
    const float32Array = base64toFloat32Array(base64);
    self.postMessage(float32Array, [float32Array.buffer]);
  };
} else {
  workerPool.push(
    ...new Array(5).fill(null).map(
      (_, i) =>
        new Worker("base64toFloat32Array.worker.mjs", {
          name: `worker-${i}`,
          type: "module",
        })
    )
  );
}

/**
 * @template T
 * @param {(worker: Worker) => T | Promise<T>} cb
 * @returns {Promise<T>}
 */
function tryGetWorker(cb) {
  /**
   * @param {Worker} worker
   */
  const resetWorker = (worker) => {
    worker.onmessage = null;
    worker.onerror = null;
    workerPool.push(worker);
  };
  return new Promise((resolve, rej) => {
    const interval = setInterval(() => {
      const worker = workerPool.pop();
      if (!worker) return;

      clearInterval(interval);
      try {
        const result = cb(worker);
        if (result instanceof Promise) {
          result.then(resolve, rej).finally(() => resetWorker(worker));
        } else {
          resolve(result);
          resetWorker(worker);
        }
      } catch (error) {
        rej(error);
        resetWorker(worker);
      }
    }, 0);
  });
}

/**
 * Ideally, we should use a pool of workers to process the data
 * @param {string} base64
 * @returns {Promise<Float32Array>}
 */
export function base64toFloat32ArrayWorker(base64) {
  return tryGetWorker((worker) => {
    return new Promise((resolve, rej) => {
      worker.onmessage = (event) => resolve(event.data);
      worker.onerror = (event) => rej(event);
      worker.postMessage(base64);
    });
  });
}
