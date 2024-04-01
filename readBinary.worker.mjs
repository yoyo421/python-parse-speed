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
    JSON: 201,
  };
  static FIELD_TYPE_SIZE = {
    [AIMSink.FIELD_TYPE.NAN]: 0,
    [AIMSink.FIELD_TYPE.KEY_OR_UTF8]: 1,
    [AIMSink.FIELD_TYPE.ARR_FLOAT32]: 4,
    [AIMSink.FIELD_TYPE.JSON]: 1,
  };
  constructor(bufferSize = 1024 * 16) {
    this.buffer = new ArrayBuffer(bufferSize);
    this.bytes = new Uint8Array(this.buffer);
    this.viewer = new DataView(this.buffer);
    /**
     * @type {{
     *  fields: Record<string, any | any[]>,
     * eof_data: any
     * }}
     */
    this.payload = {
      fields: {},
      eof_data: null,
    };
    this.decoder = new TextDecoder();
    this.buffer_populated_size = 0;
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
    this.buffer_populated_size = 0;
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
      this.buffer_populated_size,
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
   * @returns {boolean}
   */
  _read_stream_header() {
    if (this.buffer_populated_size < 4) {
      throw new Error("Unexpected EOF");
    }
    const fileHeader = this.bytes.slice(0, 3);
    this.stream_version = this.viewer.getUint8(3);
    this._reading_offset = 4;
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
   * @returns {boolean}
   */
  _read_header() {
    if (this.buffer_populated_size < 1) {
      return false;
    }
    this.field_type = this.viewer.getUint8(0);
    if (
      this.field_type < AIMSink.FIELD_TYPE.JSON &&
      this.field_type > AIMSink.FIELD_TYPE.NAN
    ) {
      if (this.buffer_populated_size < 5) {
        return false;
      }
      this.field_byte_length = this.viewer.getUint32(1);
      this._reading_offset = 5;
      this._next_readmode();
      if (this.field_type === AIMSink.FIELD_TYPE.KEY_OR_UTF8) {
        this.string_buffer = "";
      }
    } else if (this.field_type >= AIMSink.FIELD_TYPE.JSON) {
      this.readmode = AIMSink.READ_MODE.EOF;
      this._reading_offset = 1;
    }
    // calculate field length
    this.value_byte_size = AIMSink.FIELD_TYPE_SIZE[this.field_type];
    this.field_byte_length *= this.value_byte_size;
    return true;
  }

  /**
   * @returns {Uint8Array}
   */
  _read_bytes_by_populated_size() {
    let fieldData = null;
    if (this.buffer_populated_size < this.field_byte_length) {
      const buffer_can_alloc =
        this.buffer_populated_size -
        (this.buffer_populated_size % this.value_byte_size);
      fieldData = this.bytes.subarray(0, buffer_can_alloc);
      this._reading_offset = buffer_can_alloc;
    } else {
      fieldData = this.bytes.subarray(0, this.field_byte_length);
      this._reading_offset = this.field_byte_length;
    }
    return fieldData;
  }

  /**
   * @param {Uint8Array} data
   */
  _read_data(data) {
    switch (this.field_type) {
      case AIMSink.FIELD_TYPE.KEY_OR_UTF8:
        if (this.field_byte_length > data.byteLength) {
          this.string_buffer += this.decoder.decode(data, {
            stream: true,
          });
          return;
        }
        this.string_buffer += this.decoder.decode(data, {
          stream: false,
        });
        if (this.readmode === AIMSink.READ_MODE.DATA_KEY) {
          this.last_key = this.string_buffer;
          this.payload.fields[this.last_key] = null;
        } else if (this.readmode === AIMSink.READ_MODE.DATA_VALUE) {
          this.payload.fields[this.last_key] = this.string_buffer;
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
        const field = this.payload.fields[this.last_key];
        const viewer = new DataView(data.buffer, 0, data.byteLength);
        const viewer_length = viewer.byteLength;
        let i_bytes = 0;
        let i = field.length - this.field_byte_length / this.value_byte_size;
        while (i_bytes < viewer_length) {
          // LITTLE ENDIAN ONLY, change getFloat32 for other array field types
          field[i] = viewer.getFloat32(i_bytes);
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
      case AIMSink.FIELD_TYPE.JSON:
        if (this.buffer_populated_size > 0) {
          this.string_buffer += this.decoder.decode(
            this.bytes.subarray(0, this.buffer_populated_size),
            { stream: true }
          );
          this._reading_offset = this.buffer_populated_size;
        }
        return;
      default:
        throw new Error("Invalid field type");
    }
  }

  /**
   * @returns {boolean}
   */
  readFromBuffer() {
    this.debug(...this.flag());
    this._reading_offset = 0;
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
        const fieldData = this._read_bytes_by_populated_size();
        this._read_data(fieldData);
        this.field_byte_length -= fieldData.byteLength;
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
    if (this._reading_offset === 0) {
      return false;
    }

    // Move the buffer to the front
    this.buffer_populated_size -= this._reading_offset;
    this.bytes.copyWithin(0, this._reading_offset);

    // If we dont have enough data for the next read, we need to fetch more data
    if (this.buffer_populated_size === 0) {
      return false;
    }
    return true;
  }

  /**
   * @param {Uint8Array} chunk
   * @param {WritableStreamDefaultController<Uint8Array>} controller
   */
  write(chunk, controller) {
    this.debug("input chunk", chunk.byteLength);
    this.bytesRead += chunk.byteLength;
    while (true && controller.signal.aborted === false) {
      if (chunk.byteLength === 0) {
        this.bytes.fill(0, this.buffer_populated_size); // FIXME: clear buffer, debug only, remove this line
        break;
      }
      const copy_size = this.buffer.byteLength - this.buffer_populated_size;
      if (chunk.byteLength < copy_size) {
        this.bytes.fill(0, this.buffer_populated_size); // FIXME: clear buffer, debug only, remove this line
        this.bytes.set(chunk, this.buffer_populated_size);
        this.buffer_populated_size += chunk.byteLength;
        while (this.readFromBuffer()) {}
        break;
      } else {
        this.bytes.set(
          chunk.subarray(0, copy_size),
          this.buffer_populated_size
        );
        this.buffer_populated_size += copy_size;
        chunk = chunk.subarray(copy_size);
        while (this.readFromBuffer()) {}
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
      if (this.field_type === AIMSink.FIELD_TYPE.JSON) {
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

const sink = new AIMSink(1024 * 8);
/**
 * Parses binary data from a readable stream.
 * @param {ReadableStream<Uint8Array>} stream - The readable stream containing binary data.
 * @returns {Promise<void>} A promise that resolves when the parsing is complete.
 */
export async function parseBinaryData(stream) {
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
