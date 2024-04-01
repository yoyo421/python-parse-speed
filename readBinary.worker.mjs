/**
 * @file readBinary.worker.js
 */

/**
 * @type {WorkerGlobalScope}
 */
const self = globalThis;

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
    this.field_length = 0;

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
  }

  flag() {
    return [
      this.buffer_populated_size,
      this.readmode,
      this.field_type,
      this.field_length,
    ];
  }

  debug(...args) {
    if (false) {
      console.log(...args);
    }
  }

  /**
   * @returns {boolean}
   */
  readFromBuffer() {
    this.debug(...this.flag());
    let buffer_offset = 0;
    if (this.readmode === AIMSink.READ_MODE.MAGIC) {
      if (this.buffer_populated_size < 4) {
        throw new Error("Unexpected EOF");
      }
      const fileHeader = this.bytes.slice(0, 3);
      this.stream_version = this.viewer.getUint8(3);
      buffer_offset = 4;
      if (this.decoder.decode(fileHeader) !== AIMSink.MAGIC) {
        throw new Error("Invalid magic number");
      }
      if (this.stream_version > AIMSink.VERSION) {
        throw new Error("Invalid version number");
      }
      this.readmode = AIMSink.READ_MODE.HEADER_KEY;
      this.debug(AIMSink.MAGIC, AIMSink.VERSION, this.stream_version);
    } else if (
      this.readmode === AIMSink.READ_MODE.HEADER_KEY ||
      this.readmode === AIMSink.READ_MODE.HEADER_VALUE
    ) {
      // read header
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
        this.field_length = this.viewer.getUint32(1);
        buffer_offset = 5;
        if (this.readmode === AIMSink.READ_MODE.HEADER_KEY) {
          this.readmode = AIMSink.READ_MODE.DATA_KEY;
        } else if (this.readmode === AIMSink.READ_MODE.HEADER_VALUE) {
          this.readmode = AIMSink.READ_MODE.DATA_VALUE;
        } else {
          throw new Error("Invalid read mode");
        }
        if (this.field_type === AIMSink.FIELD_TYPE.KEY_OR_UTF8) {
          this.string_buffer = "";
        }
      } else if (this.field_type >= AIMSink.FIELD_TYPE.JSON) {
        this.readmode = AIMSink.READ_MODE.EOF;
        buffer_offset = 1;
      }
      // calculate field length
      if (this.field_type === AIMSink.FIELD_TYPE.KEY_OR_UTF8) {
        this.value_byte_size = 1;
        this.field_length *= 1;
      } else if (this.field_type === AIMSink.FIELD_TYPE.ARR_FLOAT32) {
        this.value_byte_size = 4;
        this.field_length *= 4;
      } else if (this.field_type === AIMSink.FIELD_TYPE.JSON) {
        this.value_byte_size = 1;
        this.field_length *= 1;
      }
    } else if (
      this.readmode === AIMSink.READ_MODE.DATA_KEY ||
      this.readmode === AIMSink.READ_MODE.DATA_VALUE
    ) {
      // read data
      /** @type {Uint8Array} */
      let fieldData = null;
      if (this.buffer_populated_size < this.field_length) {
        const buffer_can_alloc =
          this.buffer_populated_size -
          (this.buffer_populated_size % this.value_byte_size);
        fieldData = this.bytes.subarray(0, buffer_can_alloc);
        buffer_offset = buffer_can_alloc;
      } else {
        fieldData = this.bytes.subarray(0, this.field_length);
        buffer_offset = this.field_length;
      }
      if (this.field_type === AIMSink.FIELD_TYPE.KEY_OR_UTF8) {
        // fieldData is not complete
        if (this.field_length - fieldData.byteLength > 0) {
          this.string_buffer += this.decoder.decode(fieldData, {
            stream: true,
          });
        } else {
          this.string_buffer += this.decoder.decode(fieldData, {
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
        }
      } else if (this.field_type === AIMSink.FIELD_TYPE.ARR_FLOAT32) {
        // Any numeric array
        if (!this.payload.fields[this.last_key]) {
          this.payload.fields[this.last_key] = new Array(
            this.field_length / this.value_byte_size
          );
        }
        const field = this.payload.fields[this.last_key];
        const viewer = new DataView(fieldData.buffer, 0, fieldData.byteLength);
        let i_bytes = 0;
        let i = field.length - this.field_length / this.value_byte_size;
        const viewer_length = viewer.byteLength;
        while (i_bytes < viewer_length) {
          field[i] = viewer.getFloat32(i_bytes);
          i_bytes += this.value_byte_size;
          i++;
        }
      } else {
        throw new Error("Invalid field type");
      }
      this.field_length -= fieldData.byteLength;
      if (this.field_length === 0) {
        if (this.readmode === AIMSink.READ_MODE.DATA_KEY) {
          this.readmode = AIMSink.READ_MODE.HEADER_VALUE;
        } else if (this.readmode === AIMSink.READ_MODE.DATA_VALUE) {
          this.readmode = AIMSink.READ_MODE.HEADER_KEY;
        }
      }
    } else if (this.readmode === AIMSink.READ_MODE.EOF) {
      if (this.field_type === AIMSink.FIELD_TYPE.JSON) {
        if (this.buffer_populated_size > 0) {
          this.string_buffer += this.decoder.decode(
            this.bytes.subarray(0, this.buffer_populated_size),
            { stream: true }
          );
          buffer_offset = this.buffer_populated_size;
        }
      } else {
        throw new Error("Invalid field type");
      }
    } else {
      throw new Error("Invalid readmode");
    }
    if (buffer_offset === 0) {
      return false;
    }
    this.buffer_populated_size -= buffer_offset;
    this.bytes.copyWithin(0, buffer_offset);
    if (this.buffer_populated_size === 0) {
      return false;
    }
    return true;
  }

  /**
   * @param {Uint8Array} chunk
   */
  write(chunk) {
    this.debug("input chunk", chunk.byteLength);
    this.bytesRead += chunk.byteLength;
    while (true) {
      if (chunk.byteLength === 0) {
        this.bytes.fill(0, this.buffer_populated_size); // FIXME: clear buffer, debug only, remove this line
        this.buffer_populated_size = 0;
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

  close() {
    if (this.readmode === AIMSink.READ_MODE.EOF) {
      if (this.field_type === AIMSink.FIELD_TYPE.JSON) {
        this.payload.eof_data = JSON.parse(this.string_buffer);
      }
    }
    this.debug("closed");
  }

  /** @returns {WritableStream<Uint8Array>} */
  getWriter() {
    return new WritableStream(this, { highWaterMark: this.buffer.byteLength });
  }
}

/**
 * Parses binary data from a readable stream.
 * @param {ReadableStream<Uint8Array>} stream - The readable stream containing binary data.
 * @returns {Promise<void>} A promise that resolves when the parsing is complete.
 */
export async function parseBinaryData(stream) {
  if (stream instanceof ReadableStream && stream.locked) {
    throw new Error("The stream is not a byte stream");
  }
  const sink = new AIMSink(1024 * 8);
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
