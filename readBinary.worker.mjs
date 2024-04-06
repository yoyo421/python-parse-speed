// @ts-check
/**
 * @file readBinary.worker.js
 */

class Node {
  /** @type {{} | []} */
  value;
  /** @type {Node | null} */
  prev;

  /**
   * @param {{} | []} value
   */
  constructor(value) {
    this.value = value;
    this.prev = null;
  }

  add_next(node) {
    node.prev = this;
  }

  remove() {
    this.prev = null;
  }
}

class Payload {
  constructor() {
    this.data = new Node({});
    this.eof_data = null;
  }

  set_field(key, value) {
    this.data.value[key] = value;
  }

  get_field(key) {
    return this.data.value[key];
  }

  /**
   *
   * @param {string | null} key
   * @param {{} | [] | undefined | null} value
   * @returns
   */
  enter_scope(key, value = null) {
    const node = new Node(value ? value : {});
    // if (key in this.data.value) node = new Node(this.data.value[key]);
    // else node = new Node({});
    this.data.add_next(node);
    if (key) this.data.value[key] = node.value;
    this.data = node;
    return node.value;
  }

  /** @returns {boolean} success */
  exit_scope() {
    const current = this.data;
    if (current.prev) {
      this.data = current.prev;
      current.remove();
      return true;
    }
    return false;
  }

  clear() {
    while (this.exit_scope()) {}
    this.data = new Node({});
    this.eof_data = null;
  }
}

/**
 * @implements {UnderlyingSink<Uint8Array>}
 */
export class AIMSink {
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
  // This should be the same with the encoder
  static FIELD_TYPE = {
    NAN: -1,
    UINT8: 0,
    UINT16: 1,
    KEY_OR_UINT32: 2,
    INT8: 3,
    INT16: 4,
    INT32: 5,
    KEY_OR_UTF8: 6,
    FLOAT32: 7,
    DOUBLE: 8,
    BOOL: 9,
    ARR_UINT8: 10,
    ARR_BOOL: 11,
    ARR_FLOAT32: 12,
    ARR_DOUBLE: 13,
    EOF: 200,
    EOF_JSON: 201,
  };
  static FIELD_TYPE_SIZE = {
    [AIMSink.FIELD_TYPE.NAN]: 0,
    // single value are always 0
    [AIMSink.FIELD_TYPE.UINT8]: 0,
    [AIMSink.FIELD_TYPE.UINT16]: 0,
    [AIMSink.FIELD_TYPE.KEY_OR_UINT32]: 0,
    [AIMSink.FIELD_TYPE.INT8]: 0,
    [AIMSink.FIELD_TYPE.INT16]: 0,
    [AIMSink.FIELD_TYPE.INT32]: 0,
    [AIMSink.FIELD_TYPE.KEY_OR_UTF8]: 0,
    [AIMSink.FIELD_TYPE.FLOAT32]: 0,
    [AIMSink.FIELD_TYPE.DOUBLE]: 0,
    [AIMSink.FIELD_TYPE.BOOL]: 0,
    // array values
    [AIMSink.FIELD_TYPE.ARR_UINT8]: 1,
    [AIMSink.FIELD_TYPE.ARR_BOOL]: 1 / 8,
    [AIMSink.FIELD_TYPE.ARR_FLOAT32]: 4,
    [AIMSink.FIELD_TYPE.ARR_DOUBLE]: 8,
    // EOF
    [AIMSink.FIELD_TYPE.EOF]: 0,
    [AIMSink.FIELD_TYPE.EOF_JSON]: 1,
  };
  /**
   * @type {Record<number, (args: {arr: any[], dataview: DataView, i: number, i_bytes: number, bytes: Uint8Array, field_length: number}) => void>}
   */
  static FIELD_ARR_READER_FN = {
    [AIMSink.FIELD_TYPE.ARR_UINT8]: ({ arr, dataview, i, i_bytes }) =>
      (arr[i] = dataview.getUint8(i_bytes)),
    [AIMSink.FIELD_TYPE.ARR_BOOL]: ({
      arr,
      i,
      i_bytes,
      bytes,
      field_length,
    }) => {
      const i8 = i * 8;
      const byte = bytes[i_bytes];

      if (i8 + 8 > field_length) {
        for (let j = 0; i8 + j < field_length; j++) {
          arr[i8 + j] = (byte & (1 << j)) !== 0;
        }
      }

      arr[i8 + 0] = (byte & 1) !== 0;
      arr[i8 + 1] = (byte & 2) !== 0;
      arr[i8 + 2] = (byte & 4) !== 0;
      arr[i8 + 3] = (byte & 8) !== 0;
      arr[i8 + 4] = (byte & 16) !== 0;
      arr[i8 + 5] = (byte & 32) !== 0;
      arr[i8 + 6] = (byte & 64) !== 0;
      arr[i8 + 7] = (byte & 128) !== 0;
    },
    [AIMSink.FIELD_TYPE.ARR_FLOAT32]: ({ arr, dataview, i, i_bytes }) =>
      (arr[i] = dataview.getFloat32(i_bytes)),
    [AIMSink.FIELD_TYPE.ARR_DOUBLE]: ({ arr, dataview, i, i_bytes }) =>
      (arr[i] = dataview.getFloat64(i_bytes)),
  };
  /**
   * @type {Record<number, new (length: number) => RelativeIndexable>}
   */
  static FIELD_ARR_CONSTRUCTOR = {
    [AIMSink.FIELD_TYPE.ARR_UINT8]: Uint8Array,
    [AIMSink.FIELD_TYPE.ARR_BOOL]: Uint8Array,
    [AIMSink.FIELD_TYPE.ARR_FLOAT32]: Float32Array,
    [AIMSink.FIELD_TYPE.ARR_DOUBLE]: Float64Array,
  };
  /**
   * If an array need special treatment after writing, we can define it here
   * @type {Record<number, (arr: any[]) => any[]>}
   */
  static FIELD_ARR_CLOSER = {
    [AIMSink.FIELD_TYPE.ARR_BOOL]: (arr) => {
      const length = arr.length;
      const result = new Array(length);
      for (let i = 0; i < length; i++) result[i] = arr[i] !== 0;
      return result;
    },
  };
  static READ_DATA_RETURN_FLAGS = {
    CONTINUE_READING: 0,
    READ_INPLACE: 1,
  };
  constructor(bufferSize = 1024 * 16) {
    this.buffer = new ArrayBuffer(bufferSize);
    this.bytes = new Uint8Array(this.buffer);
    this.viewer = new DataView(this.buffer);
    /**
     * @type {Payload}
     */
    this.payload = new Payload();

    this.decoder = new TextDecoder();
    /** buffer_populated_size */
    this.bpsize = 0;
    this.stream_version = -1;

    /** @type {string | number | null} */
    this.last_key = null;
    this.string_buffer = "";

    this.field_type = AIMSink.FIELD_TYPE.NAN;

    this.value_size_in_bytes = 1;
    this.value_byte_size = 1;
    /**
     * Fields can span multiple packets, so we need to keep track of the length.
     *
     * For single value items, this is the value itself. this includes 64-bit values
     *
     * In static languages, this is 8 bytes, and it is versitile to store any numeric value, so 64-bit is the default
     */
    this.field_length = 0;
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

    /** @type {readonly [AIMSink]} */
    this.ptr = [this];

    /** @type {(...args) => void}  */
    this.debug = false ? console.log : () => {};
  }

  reset() {
    this.payload.clear();
    this.bpsize = 0;
    this.stream_version = -1;
    this.last_key = null;
    this.string_buffer = "";
    this.field_type = AIMSink.FIELD_TYPE.NAN;
    this.value_size_in_bytes = 8;
    this.value_byte_size = 1;
    this.field_length = 0;
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
      this.field_length,
      this.field_byte_length,
    ];
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
    // maximum header size is 5 bytes
    if (this.bpsize < 1) {
      return false;
    }
    this.field_type = this.viewer.getUint8(this._reading_offset);
    // add switch for field type
    switch (this.field_type) {
      case AIMSink.FIELD_TYPE.INT8:
      case AIMSink.FIELD_TYPE.INT16:
      case AIMSink.FIELD_TYPE.INT32:
        if (this.bpsize < 5) {
          return false;
        }
        this.field_length = this.viewer.getInt32(this._reading_offset + 1);
        this._reading_offset += 5;
        break;
      case AIMSink.FIELD_TYPE.UINT8:
      case AIMSink.FIELD_TYPE.UINT16:
      case AIMSink.FIELD_TYPE.KEY_OR_UINT32:
      case AIMSink.FIELD_TYPE.KEY_OR_UTF8:
      case AIMSink.FIELD_TYPE.BOOL:
      case AIMSink.FIELD_TYPE.ARR_UINT8:
      case AIMSink.FIELD_TYPE.ARR_BOOL:
      case AIMSink.FIELD_TYPE.ARR_FLOAT32:
      case AIMSink.FIELD_TYPE.ARR_DOUBLE:
        if (this.bpsize < 5) {
          return false;
        }
        this.field_length = this.viewer.getUint32(this._reading_offset + 1);
        this._reading_offset += 5;
        break;
      case AIMSink.FIELD_TYPE.FLOAT32:
        if (this.bpsize < 5) {
          return false;
        }
        this.field_length = this.viewer.getFloat32(this._reading_offset + 1);
        this._reading_offset += 5;
        break;
      // Special case for double, because it is 8 bytes
      case AIMSink.FIELD_TYPE.DOUBLE:
        if (this.bpsize < 9) {
          return false;
        }
        this.field_length = this.viewer.getFloat64(this._reading_offset + 1);
        this._reading_offset += 9;
        break;
      case AIMSink.FIELD_TYPE.EOF:
      case AIMSink.FIELD_TYPE.EOF_JSON:
        this.readmode = AIMSink.READ_MODE.EOF;
        this._reading_offset += 1;
        break;
      default:
        throw new Error("Invalid field type");
    }
    this.string_buffer = "";
    this._next_readmode();
    // calculate field length
    this.value_size_in_bytes = AIMSink.FIELD_TYPE_SIZE[this.field_type];
    this.value_byte_size = Math.ceil(this.value_size_in_bytes);
    this.field_byte_length = Math.ceil(
      this.field_length * this.value_size_in_bytes
    );
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

  _set_data_value(value) {
    switch (this.readmode) {
      case AIMSink.READ_MODE.DATA_KEY:
        this.last_key = value;
        return;
      case AIMSink.READ_MODE.DATA_VALUE:
        this.payload.set_field(this.last_key, value);
        return;
      default:
        throw new Error("Invalid read mode");
    }
  }

  /**
   * @param {number} bytes_to_read
   * @returns {number} `READ_DATA_RETURN_FLAGS` should we keep reading from current buffer
   */
  _read_data(bytes_to_read) {
    switch (this.field_type) {
      case AIMSink.FIELD_TYPE.BOOL:
        this._set_data_value(this.field_length === 1);
        return AIMSink.READ_DATA_RETURN_FLAGS.READ_INPLACE;
      case AIMSink.FIELD_TYPE.UINT8:
      case AIMSink.FIELD_TYPE.INT8:
        this._set_data_value(this.field_length & 0xff);
        return AIMSink.READ_DATA_RETURN_FLAGS.READ_INPLACE;
      case AIMSink.FIELD_TYPE.UINT16:
      case AIMSink.FIELD_TYPE.INT16:
        this._set_data_value(this.field_length & 0xffff);
        return AIMSink.READ_DATA_RETURN_FLAGS.READ_INPLACE;
      case AIMSink.FIELD_TYPE.KEY_OR_UINT32:
      case AIMSink.FIELD_TYPE.INT32:
        this._set_data_value(this.field_length & 0xffffffff);
        return AIMSink.READ_DATA_RETURN_FLAGS.READ_INPLACE;
      case AIMSink.FIELD_TYPE.FLOAT32:
      case AIMSink.FIELD_TYPE.DOUBLE:
        this._set_data_value(this.field_length);
        return AIMSink.READ_DATA_RETURN_FLAGS.READ_INPLACE;
      case AIMSink.FIELD_TYPE.KEY_OR_UTF8:
        const section = this.bytes.subarray(
          this._reading_offset,
          this._reading_offset + bytes_to_read
        );
        if (this.field_byte_length > bytes_to_read) {
          this.string_buffer += this.decoder.decode(section, {
            stream: true,
          });
          break; // We read all the data
        }
        this.string_buffer += this.decoder.decode(section, {
          stream: false,
        });
        this._set_data_value(this.string_buffer);
        this.string_buffer = "";
        break;
      // Any numeric array
      case AIMSink.FIELD_TYPE.ARR_UINT8:
      case AIMSink.FIELD_TYPE.ARR_BOOL:
      case AIMSink.FIELD_TYPE.ARR_FLOAT32:
      case AIMSink.FIELD_TYPE.ARR_DOUBLE:
        if (!this.payload.get_field(this.last_key)) {
          this.payload.set_field(
            this.last_key,
            new AIMSink.FIELD_ARR_CONSTRUCTOR[this.field_type](
              this.field_length
            )
          );
        }
        const byteReader = AIMSink.FIELD_ARR_READER_FN[this.field_type];
        // FIXME: This should never happen
        if (!byteReader) {
          throw new Error("Invalid field type");
        }
        const field = this.payload.get_field(this.last_key);
        const read_bytes_to_here = this._reading_offset + bytes_to_read;
        let i_bytes = this._reading_offset;
        let i = field.length - this.field_length;
        const args = {
          arr: field,
          dataview: this.viewer,
          i,
          i_bytes,
          bytes: this.bytes,
          field_length: this.field_length,
        };
        while (i_bytes < read_bytes_to_here) {
          byteReader(args);
          args.i_bytes = i_bytes += this.value_byte_size;
          args.i = ++i;
        }
        break;
      default:
        throw new Error("Invalid field type");
    }
    return AIMSink.READ_DATA_RETURN_FLAGS.CONTINUE_READING;
  }

  _close_array_field() {
    if (AIMSink.FIELD_ARR_CLOSER.hasOwnProperty(this.field_type)) {
      const field = this.payload.get_field(this.last_key);
      this.payload.set_field(
        this.last_key,
        AIMSink.FIELD_ARR_CLOSER[this.field_type](field)
      );
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
    readmode_switch: switch (this.readmode) {
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
        switch (this._read_data(bytes_to_read)) {
          case AIMSink.READ_DATA_RETURN_FLAGS.READ_INPLACE:
            this._next_readmode();
            break readmode_switch;
          case AIMSink.READ_DATA_RETURN_FLAGS.CONTINUE_READING:
            this._reading_offset += bytes_to_read;
            this.field_byte_length -= bytes_to_read;
            this.field_length -= bytes_to_read / this.value_size_in_bytes;
            if (this.field_length <= 0) {
              this._close_array_field();
              this._next_readmode();
            }
            break readmode_switch;
        }
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
    while (this.payload.exit_scope()) {}
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

let sink = new AIMSink(1024 * 16);
/**
 * Parses binary data from a readable stream.
 * @param {ReadableStream<Uint8Array>} stream - The readable stream containing binary data.
 * @returns {Promise<{
 *  data: Record<any, any>,
 *  request: any,
 *  _size: number
 * } | undefined>} A promise that resolves when the parsing is complete.
 */
export async function parseBinaryDataFromStream(stream) {
  if (stream instanceof ReadableStream && stream.locked) {
    throw new Error("The stream is not a byte stream");
  }
  try {
    const parseProcess = performance.now();
    await stream.pipeTo(sink.getWriter());
    const parseProcessEnd = performance.now();
    performance.measure(`binary - parseProcess`, {
      start: parseProcess,
      end: parseProcessEnd,
    });
    return {
      data: sink.payload.data.value,
      request: sink.payload.eof_data,
      _size: sink.bytesRead,
    };
  } catch (e) {
    sink.abort(e);
    console.error(e);
  }
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
      data: sink.payload.data.value,
      request: sink.payload.eof_data,
      _size: sink.bytesRead,
    };
  } catch (e) {
    sink.abort(e);
    console.error(e);
  }
}
