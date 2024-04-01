/// <reference lib="webworker" />
/**
 * @file readBinary.worker.js
 */

/**
 * @type {WorkerGlobalScope}
 */
const self = globalThis;

/**
 * @implements {UnderlyingSink<Uint8Array>}
 */
class AIMSink {
  static MAGIC = "AIM";
  static VERSION = 1;
  static READ_MODE = {
    MAGIC: 0,
    HEADER_KEY: 1,
    HEADER_VALUE: 2,
    DATA_KEY: 3,
    DATA_VALUE: 4,
    EOF: 5,
  };
  static FIELD_TYPE = {
    NAN: -1,
    KEY_OR_UTF8: 0,
    ARR_FLOAT32: 1,
    EOF: 200,
    EOF_JSON: 201,
  };
  static FIELD_TYPE_SIZE = {
    [AIMSink.FIELD_TYPE.NAN]: 0,
    [AIMSink.FIELD_TYPE.KEY_OR_UTF8]: 1,
    [AIMSink.FIELD_TYPE.ARR_FLOAT32]: 4,
    [AIMSink.FIELD_TYPE.EOF]: 0,
    [AIMSink.FIELD_TYPE.EOF_JSON]: 1,
  };
  /**
   * @type {Record<number, (arr: any[], dataview: DataView, i: number, i_bytes: number) => void>}
   */
  static FIELD_ARR_READER_FN = {
    [AIMSink.FIELD_TYPE.ARR_FLOAT32]: (arr, dataview, i, i_bytes) =>
      (arr[i] = dataview.getFloat32(i_bytes)),
  };
  constructor(bufferSize = 1024 * 16) {
    this.buffer = new ArrayBuffer(bufferSize);
    this.bytes = new Uint8Array(this.buffer);
    this.viewer = new DataView(this.buffer);
    /**
     * @type {{
     *  fields: Record<string, any | any[]>,
     *  eof_data: any
     * }}
     */
    this.payload = {
      fields: {},
      eof_data: null,
    };
    this.decoder = new TextDecoder();
    /** buffer_populated_size */
    this.bpsize = 0;
    this.stream_version = -1;

    this.last_key = "";
    this.string_buffer = "";

    this.field_type = AIMSink.FIELD_TYPE.NAN;

    this.value_byte_size = 1;
    /**
     * Fields can span multiple packets, so we need to keep track of the length.
     */
    this.field_byte_length = 0;

    /**
     * 0: reading magic number & version \
     * 1: reading HEADER for key / EOF HEADER \
     * 2: reading HEADER for value \
     * 3: reading data for key \
     * 4: reading data for value \
     * 5: reading EOF data
     */
    this.readmode = AIMSink.READ_MODE.MAGIC;

    this.bytesRead = 0;
    /**
     * @internal
     */
    this._reading_offset = 0;
    /**
     * Indicate the Sink status
     * 0: Ready
     * 1: In-Use
     */
    this.status = 0;
  }

  reset() {
    this.payload = { fields: {}, eof_data: null };
    this.bpsize = 0;
    this.stream_version = -1;
    this.last_key = "";
    this.string_buffer = "";
    this.field_type = AIMSink.FIELD_TYPE.NAN;
    this.value_byte_size = 1;
    this.field_byte_length = 0;
    this.readmode = AIMSink.READ_MODE.MAGIC;
    this.bytesRead = 0;
    this._reading_offset = 0;
  }

  flag() {
    return [
      this.bpsize,
      this.readmode,
      this.field_type,
      this.field_byte_length,
    ];
  }

  debug(...args) {
    if (false) {
      console.log(...args);
    }
  }

  _next_readmode() {
    switch (this.readmode) {
      case AIMSink.READ_MODE.MAGIC:
        this.readmode = AIMSink.READ_MODE.HEADER_KEY;
        break;
      case AIMSink.READ_MODE.HEADER_KEY:
        this.readmode = AIMSink.READ_MODE.DATA_KEY;
        break;
      case AIMSink.READ_MODE.DATA_KEY:
        this.readmode = AIMSink.READ_MODE.HEADER_VALUE;
        break;
      case AIMSink.READ_MODE.HEADER_VALUE:
        this.readmode = AIMSink.READ_MODE.DATA_VALUE;
        break;
      case AIMSink.READ_MODE.DATA_VALUE:
        this.readmode = AIMSink.READ_MODE.HEADER_KEY;
        break;
    }
  }

  /**
   * @returns {boolean} should we keep reading from current buffer
   */
  _read_stream_header() {
    if (this.bpsize < 4) {
      throw new Error("Unexpected EOF");
    }
    const fileHeader = this.bytes.slice(this._reading_offset, 3);
    this.stream_version = this.viewer.getUint8(this._reading_offset + 3);
    this._reading_offset += 4;
    if (this.decoder.decode(fileHeader) !== AIMSink.MAGIC) {
      throw new Error("Invalid magic number");
    }
    if (this.stream_version > AIMSink.VERSION) {
      throw new Error("Invalid version number");
    }
    this.readmode = AIMSink.READ_MODE.HEADER_KEY;
    this.debug(AIMSink.MAGIC, AIMSink.VERSION, this.stream_version);
    return true;
  }

  /**
   * @returns {boolean} should we keep reading from current buffer
   */
  _read_header() {
    if (this.bpsize < 1) {
      return false;
    }
    this.field_type = this.viewer.getUint8(this._reading_offset);
    if (
      this.field_type < AIMSink.FIELD_TYPE.EOF &&
      this.field_type > AIMSink.FIELD_TYPE.NAN
    ) {
      if (this.bpsize < 5) {
        return false;
      }
      this.field_byte_length = this.viewer.getUint32(this._reading_offset + 1);
      this._reading_offset += 5;
      this._next_readmode();
      if (this.field_type === AIMSink.FIELD_TYPE.KEY_OR_UTF8) {
        this.string_buffer = "";
      }
    } else if (this.field_type >= AIMSink.FIELD_TYPE.EOF) {
      this.readmode = AIMSink.READ_MODE.EOF;
      this._reading_offset += 1;
    }
    // calculate field length
    this.value_byte_size = AIMSink.FIELD_TYPE_SIZE[this.field_type];
    this.field_byte_length *= this.value_byte_size;
    return true;
  }

  /**
   * @returns {number} bytes to read
   */
  _read_bytes_by_populated_size() {
    if (this.bpsize < this.field_byte_length) {
      const buffer_can_alloc =
        this.bpsize - (this.bpsize % this.value_byte_size);
      return buffer_can_alloc;
    } else {
      return this.field_byte_length;
    }
  }

  /**
   * @param {number} bytes_to_read
   */
  _read_data(bytes_to_read) {
    switch (this.field_type) {
      case AIMSink.FIELD_TYPE.KEY_OR_UTF8:
        const section = this.bytes.subarray(
          this._reading_offset,
          this._reading_offset + bytes_to_read
        );
        if (this.field_byte_length > bytes_to_read) {
          this.string_buffer += this.decoder.decode(section, {
            stream: true,
          });
          return;
        }
        this.string_buffer += this.decoder.decode(section, {
          stream: false,
        });
        if (this.readmode === AIMSink.READ_MODE.DATA_KEY) {
          this.last_key = this.string_buffer;
          this.payload.fields[this.last_key] = null;
        } else if (this.readmode === AIMSink.READ_MODE.DATA_VALUE) {
          switch (this.field_type) {
            case AIMSink.FIELD_TYPE.KEY_OR_UTF8:
              this.payload.fields[this.last_key] = this.string_buffer;
              break;
            default:
              throw new Error("Invalid field type");
          }
        } else {
          throw new Error("Invalid read mode");
        }
        this.string_buffer = "";
        return;
      // Any numeric array
      case AIMSink.FIELD_TYPE.ARR_FLOAT32:
        if (!this.payload.fields[this.last_key]) {
          this.payload.fields[this.last_key] = new Array(
            this.field_byte_length / this.value_byte_size
          );
        }
        const byteReader = AIMSink.FIELD_ARR_READER_FN[this.field_type];
        // FIXME: This should never happen
        if (!byteReader) {
          throw new Error("Invalid field type");
        }
        const field = this.payload.fields[this.last_key];
        const read_bytes_to_here = this._reading_offset + bytes_to_read;
        let i_bytes = this._reading_offset;
        let i = field.length - this.field_byte_length / this.value_byte_size;
        while (i_bytes < read_bytes_to_here) {
          byteReader(field, this.viewer, i, i_bytes);
          i_bytes += this.value_byte_size;
          i++;
        }
        return;
      default:
        throw new Error("Invalid field type");
    }
  }

  _read_eof() {
    switch (this.field_type) {
      case AIMSink.FIELD_TYPE.EOF:
        this._reading_offset += this.bpsize;
        return;
      case AIMSink.FIELD_TYPE.EOF_JSON:
        if (this.bpsize > 0) {
          this.string_buffer += this.decoder.decode(
            this.bytes.subarray(
              this._reading_offset,
              this._reading_offset + this.bpsize
            ),
            { stream: true }
          );
          this._reading_offset += this.bpsize;
        }
        return;
      default:
        throw new Error("Invalid field type");
    }
  }

  /**
   * @returns {boolean} should we keep reading from current buffer
   */
  readFromBuffer() {
    this.debug(...this.flag());
    const _reading_position = this._reading_offset;
    switch (this.readmode) {
      case AIMSink.READ_MODE.MAGIC:
        if (!this._read_stream_header()) {
          return false;
        }
        break;
      case AIMSink.READ_MODE.HEADER_KEY:
      case AIMSink.READ_MODE.HEADER_VALUE:
        if (!this._read_header()) {
          return false;
        }
        break;
      case AIMSink.READ_MODE.DATA_KEY:
      case AIMSink.READ_MODE.DATA_VALUE:
        const bytes_to_read = this._read_bytes_by_populated_size();
        this._read_data(bytes_to_read);
        this._reading_offset += bytes_to_read;
        this.field_byte_length -= bytes_to_read;
        if (this.field_byte_length === 0) {
          this._next_readmode();
        }
        break;
      case AIMSink.READ_MODE.EOF:
        this._read_eof();
        break;
      default:
        throw new Error("Invalid readmode");
    }
    // If we didnt read anything, we need to fetch more data
    if (_reading_position === this._reading_offset) {
      return false;
    }

    // Remove delta from buffer_populated_size
    this.bpsize -= this._reading_offset - _reading_position;

    // Do we have more data to read?, if yes, continue reading, else return false
    return this.bpsize > 0;
  }

  // Move the buffer to the front, hence we can write more data. also helps in debugging
  reset_reading_pos() {
    this.bytes.copyWithin(0, this._reading_offset);
    this._reading_offset = 0;
  }

  /**
   * @param {Uint8Array} chunk
   * @param {{signal: AbortSignal}?} controller
   */
  write(chunk, controller) {
    this.debug("input chunk", chunk.byteLength);
    this.bytesRead += chunk.byteLength;
    /**
     * This act as while(true) loop,
     *
     * but we also need to check if the controller is aborted,
     * but because we can manually run the function, controller can be null
     *
     * so the condition is:
     *  true && (controller is not null && controller is not aborted)
     * or in other words:
     *  (controller is null || controller is not aborted)
     */
    while (!controller || controller.signal.aborted === false) {
      // If we have read all the data, we can break the loop
      if (chunk.byteLength === 0) {
        this.bytes.fill(0, this.bpsize); // FIXME: clear buffer, debug only, remove this line
        break;
      }
      // Determine the size of the buffer we can copy
      const copy_size = this.buffer.byteLength - this.bpsize;
      // If the chunk is smaller than the buffer, we can copy the whole chunk
      if (chunk.byteLength < copy_size) {
        this.bytes.fill(0, this.bpsize); // FIXME: clear buffer, debug only, remove this line
        this.bytes.set(chunk, this.bpsize);
        this.bpsize += chunk.byteLength;

        // We read, if we have more data to read, we will continue reading
        while (this.readFromBuffer()) {}
        this.reset_reading_pos();
        break;
      } else {
        // If the chunk is bigger than the buffer, we will only copy the buffer available size
        this.bytes.set(chunk.subarray(0, copy_size), this.bpsize);
        this.bpsize += copy_size;
        chunk = chunk.subarray(copy_size);

        // We read, if we have more data to read, we will continue reading
        while (this.readFromBuffer()) {}
        this.reset_reading_pos();
      }
    }
  }

  abort(reason) {
    this.status = 0;
    this.debug("aborted", reason);
  }

  /**
   *
   * @param {WritableStreamDefaultController} controller
   * @returns
   */
  start(controller) {
    if (this.status === 1) {
      controller.error(new Error("Sink is already in use"));
      return;
    }
    this.status = 1;
    this.debug("started");
  }

  close() {
    if (this.readmode === AIMSink.READ_MODE.EOF) {
      if (this.field_type === AIMSink.FIELD_TYPE.EOF_JSON) {
        this.payload.eof_data = JSON.parse(this.string_buffer);
      }
    }
    this.readmode = AIMSink.READ_MODE.MAGIC;
    this.status = 0;
    this.debug("closed");
  }

  /** @returns {WritableStream<Uint8Array>} */
  getWriter() {
    if (sink.status === 1) {
      throw new Error("Sink is already in use");
    }
    sink.reset();
    return new WritableStream(this, { highWaterMark: this.buffer.byteLength });
  }
}

const sink = new AIMSink(1024);
/**
 * Parses binary data from a readable stream.
 * @param {ReadableStream<Uint8Array>} stream - The readable stream containing binary data.
 * @returns {Promise<any>} A promise that resolves when the parsing is complete.
 */
export async function parseBinaryDataFromStream(stream) {
  if (stream instanceof ReadableStream && stream.locked) {
    throw new Error("The stream is not a byte stream");
  }
  const parseProcess = performance.now();
  await stream.pipeTo(sink.getWriter());
  const parseProcessEnd = performance.now();
  performance.measure(`binary - parseProcess`, {
    start: parseProcess,
    end: parseProcessEnd,
  });
  return {
    data: sink.payload.fields,
    request: sink.payload.eof_data,
    _size: sink.bytesRead,
  };
}

/**
 * @param {ArrayBuffer} arrayBuffer
 */
export async function parseBinaryData(arrayBuffer) {
  try {
    const buff = new Uint8Array(arrayBuffer);
    const parseProcess = performance.now();
    sink.reset();
    sink.start({ error() {} });
    sink.write(buff, { signal: { aborted: false } });
    sink.close();
    const parseProcessEnd = performance.now();
    performance.measure(`binary - parseProcess`, {
      start: parseProcess,
      end: parseProcessEnd,
    });
    return {
      data: sink.payload.fields,
      request: sink.payload.eof_data,
      _size: sink.bytesRead,
    };
  } catch (e) {
    sink.abort(e);
  }
}
