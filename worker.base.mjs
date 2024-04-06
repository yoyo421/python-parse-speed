/// <reference lib="webworker" />

/**
 * @type {WorkerGlobalScope}
 */
const self = globalThis;

/**
 * @template {Record<string, (...args) => object | Promise<object> | ArrayBufferView | Promise<ArrayBufferView>>} T
 *
 * @param {T} workerFn
 * @param {{
 *  filename: string
 *  instances?: number
 * }} options
 * @returns
 */
export const workerInstance = (workerFn, { instances = 5, filename }) => {
  const workerPool = [];
  if (globalThis["WorkerGlobalScope"] && self instanceof WorkerGlobalScope) {
    self.onmessage = async function (event) {
      const [fnKey, args] = event.data;
      const result = await workerFn[fnKey](...args);
      self.postMessage(
        result,
        typeof result === "object" && "buffer" in result ? [result.buffer] : []
      );
    };
  } else {
    // 5 workers should be enough for most cases
    workerPool.push(
      ...new Array(instances).fill(null).map(
        (_, i) =>
          new Worker(filename, {
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
   * @template {keyof T} U
   *
   * @param {U} fnKey
   * @param  {Parameters<T[U]>} args
   * @returns {Promise<ReturnType<T[U]>>}
   */
  function runFromWorker(fnKey, ...args) {
    return tryGetWorker((worker) => {
      return new Promise((resolve, rej) => {
        worker.onmessage = (event) => resolve(event.data);
        worker.onerror = (event) => rej(event);
        const objectArgs = args.filter((arg) => typeof arg === "object");
        worker.postMessage(
          [fnKey, args],
          [
            ...objectArgs.filter((arg) => arg instanceof OffscreenCanvas),
            ...objectArgs.filter((arg) => arg instanceof OffscreenCanvas),
            ...objectArgs.filter((arg) => arg instanceof MessagePort),
            ...objectArgs.filter((arg) => arg instanceof ReadableStream),
            ...objectArgs.filter((arg) => arg instanceof WritableStream),
            ...objectArgs.filter((arg) => arg instanceof TransformStream),
            ...objectArgs.filter((arg) => arg instanceof VideoFrame),
            ...objectArgs.filter((arg) => arg instanceof ArrayBuffer),
            ...objectArgs
              .filter(
                (arg) => "buffer" in arg && arg.buffer instanceof ArrayBuffer
              )
              .map((arg) => arg.buffer),
          ]
        );
      });
    });
  }

  return { tryGetWorker, runFromWorker };
};
