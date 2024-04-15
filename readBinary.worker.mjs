// @ts-check
/**
 * @file readBinary.worker.js
 */

/** @template {{} | []} T */
class Node {
  /** @type {T} */
  value;
  /** @type {Node | null} */
  prev;
  /** @type {FieldMetadata} */
  metadata;

  /**
   * @param {T} value
   * @param {FieldMetadata} metadata
   */
  constructor(value, metadata) {
    this.value = value;
    this.prev = null;
    this.metadata = metadata;
  }

  add_next(node) {
    node.prev = this;
  }

  remove() {
    this.prev = null;
  }
}

const TypedArray = Object.getPrototypeOf(Int8Array);

/**
 * @template {any[] | {}} T
 */
class FieldConstructor {
  constructor() {
    /** @type {Node<T>} */
    // @ts-ignore
    this.data = new Node({}, new FieldMetadata());
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
   * @param {string | number | null} key
   * @param {T} value
   * @returns {T}
   */
  enter_scope(key, value) {
    const node = new Node(
      value,
      new FieldMetadata(
        Array.isArray(value) || value instanceof TypedArray
          ? AIMSink.FIELD_MODE.ARRAY
          : AIMSink.FIELD_MODE.OBJECT
      )
    );
    // if (key in this.data.value) node = new Node(this.data.value[key]);
    // else node = new Node({});
    this.data.add_next(node);
    if (key) this.set_field(key, node.value);
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
    this.data.remove();
    // @ts-ignore
    this.data.value = {};
    this.data.metadata.reset();
    this.eof_data = null;
  }
}

/**
 * Little bit insane, but we can change it to DataView
 */
class FieldMetadata {
  /**
   * @param {number} mode
   */
  constructor(mode = AIMSink.FIELD_MODE.OBJECT) {
    this.type = AIMSink.FIELD_TYPE.NAN;
    this.mode = mode;
    this.last_key = null;
    /**
     * Fields can span multiple packets, so we need to keep track of the length.
     *
     * For single value items, this is the value itself. this includes 64-bit values
     *
     * In static languages, this is 8 bytes, and it is versitile to store any numeric value, so 64-bit is the default
     */
    this.length = 0;
    this.byte_length = 0;
    this.value_size_in_bytes = 1;
    this.value_byte_size = 1;
  }

  reset() {
    this.type = AIMSink.FIELD_TYPE.NAN;
    this.last_key = null;
    this.length = 0;
    this.byte_length = 0;
    this.value_size_in_bytes = 1;
    this.value_byte_size = 1;
  }

  /**
   * @param {DataView} view
   * @param {number} offset
   */
  set_field_type_from_dataview(view, offset) {
    this.type = view.getUint8(offset);
  }

  /**
   * calculate field length
   */
  process_field_type() {
    this.value_size_in_bytes = AIMSink.FIELD_TYPE_SIZE[this.type];
    this.value_byte_size = Math.ceil(this.value_size_in_bytes);
    this.byte_length = Math.ceil(this.length * this.value_size_in_bytes);
  }
}

/**
 * @template T
 * @param {Record<string, T>} ENUM
 * @param {T} value
 * @returns
 */
function getENUMName(ENUM, value) {
  return Object.keys(ENUM).find((key) => ENUM[key] === value);
}

/**
 *
 * TODO: add events for object capture and stuff
 *
 * @implements {UnderlyingSink<Uint8Array>}
 */
export class AIMSink {
  static MAGIC = "AIM";
  static VERSION = 1;
  static READ_MODE = {
    MAGIC: 0,
    KEY_FOR_HEADER: 1,
    KEY_FOR_VALUE: 2,
    DATA_FOR_HEADER: 3,
    DATA_FOR_VALUE: 4,
    EOF: 5,
  };
  // This should be the same with the encoder
  static FIELD_MODE = {
    OBJECT: 0,
    ARRAY: 1,
  };
  static FIELD_TYPE = {
    NAN: -1,
    UINT8: 0,
    UINT16: 1,
    /** available on `AIMSink.FIELD_MODE.OBJECT` or `AIMSink.FIELD_MODE.ARRAY` */
    KEY_OR_UINT32: 2,
    INT8: 3,
    INT16: 4,
    INT32: 5,
    /** available on `AIMSink.FIELD_MODE.OBJECT` */
    KEY_OR_UTF8: 6,
    FLOAT32: 7,
    DOUBLE: 8,
    BOOL: 9,
    OBJECT: 10,
    OBJECT_OR_ARR_CUSTOM_CLOSE: 12,
    /**
     * Custom implementation of array, each item can be different, always DATA_KEY -> DATA_VALUE till END OF ARRAY
     */
    ARR_CUSTOM: 13,
    ARR_UINT8: 15,
    ARR_BOOL: 16,
    ARR_FLOAT32: 17,
    ARR_DOUBLE: 18,
    EOF: 200,
    EOF_JSON: 201,
  };
  static FIELD_TYPE_SIZE = {
    [AIMSink.FIELD_TYPE.NAN]: 0,
    // single value are always 1
    [AIMSink.FIELD_TYPE.UINT8]: 1,
    [AIMSink.FIELD_TYPE.UINT16]: 2,
    [AIMSink.FIELD_TYPE.KEY_OR_UINT32]: 4,
    [AIMSink.FIELD_TYPE.INT8]: 1,
    [AIMSink.FIELD_TYPE.INT16]: 2,
    [AIMSink.FIELD_TYPE.INT32]: 4,
    [AIMSink.FIELD_TYPE.KEY_OR_UTF8]: 1,
    [AIMSink.FIELD_TYPE.FLOAT32]: 4,
    [AIMSink.FIELD_TYPE.DOUBLE]: 8,
    [AIMSink.FIELD_TYPE.BOOL]: 1,
    [AIMSink.FIELD_TYPE.OBJECT]: 1,
    [AIMSink.FIELD_TYPE.ARR_CUSTOM]: 1,
    [AIMSink.FIELD_TYPE.OBJECT_OR_ARR_CUSTOM_CLOSE]: 1,
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
    [AIMSink.FIELD_TYPE.ARR_CUSTOM]: Array,
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
    // [AIMSink.FIELD_TYPE.ARR_FLOAT32]: (arr) => {
    //   const length = arr.length;
    //   const result = new Array(length);
    //   for (let i = 0; i < length; i++) result[i] = arr[i];
    //   return result;
    // },
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
     * @type {FieldConstructor}
     */
    this.payload = new FieldConstructor();

    this.decoder = new TextDecoder();
    /** buffer_populated_size */
    this.bpsize = 0;
    this.stream_version = -1;

    /** @type {string | number | null} */
    this.string_buffer = "";
    this.string_byte_length = 0;

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
    this.debug = true ? console.log : () => {};
  }

  reset() {
    this.payload.clear();
    this.bpsize = 0;
    this.stream_version = -1;
    this.string_buffer = "";
    this.string_byte_length = 0;
    this.readmode = AIMSink.READ_MODE.MAGIC;
    this.bytesRead = 0;
    this._reading_offset = 0;
  }

  flag() {
    return [
      this.bpsize,
      this.readmode,
      this.payload.data.metadata.type,
      this.payload.data.metadata.length,
      this.payload.data.metadata.byte_length,
    ];
  }

  _next_readmode() {
    if (this.payload.data.metadata.mode === AIMSink.FIELD_MODE.ARRAY) {
      switch (this.readmode) {
        case AIMSink.READ_MODE.KEY_FOR_HEADER:
        case AIMSink.READ_MODE.DATA_FOR_HEADER:
        case AIMSink.READ_MODE.KEY_FOR_VALUE:
          this.readmode = AIMSink.READ_MODE.DATA_FOR_VALUE;
          break;
        case AIMSink.READ_MODE.DATA_FOR_VALUE:
          this.readmode = AIMSink.READ_MODE.KEY_FOR_VALUE;
          break;
        default:
          throw new Error("Invalid read mode");
      }
      return;
    }
    switch (this.readmode) {
      case AIMSink.READ_MODE.MAGIC:
        this.readmode = AIMSink.READ_MODE.KEY_FOR_HEADER;
        break;
      case AIMSink.READ_MODE.KEY_FOR_HEADER:
        this.readmode = AIMSink.READ_MODE.DATA_FOR_HEADER;
        break;
      case AIMSink.READ_MODE.DATA_FOR_HEADER:
        this.readmode = AIMSink.READ_MODE.KEY_FOR_VALUE;
        break;
      case AIMSink.READ_MODE.KEY_FOR_VALUE:
        this.readmode = AIMSink.READ_MODE.DATA_FOR_VALUE;
        break;
      case AIMSink.READ_MODE.DATA_FOR_VALUE:
        this.readmode = AIMSink.READ_MODE.KEY_FOR_HEADER;
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
    this.readmode = AIMSink.READ_MODE.KEY_FOR_HEADER;
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
    const field_info = this.payload.data.metadata;

    // Special case for field mode array, we already know the key
    if (
      field_info.mode === AIMSink.FIELD_MODE.ARRAY &&
      this.readmode === AIMSink.READ_MODE.KEY_FOR_HEADER
    ) {
      this.readmode = AIMSink.READ_MODE.KEY_FOR_VALUE;
      return true;
    }

    const viewer = this.viewer;
    field_info.type = viewer.getUint8(this._reading_offset);
    // add switch for field type
    if (this.readmode === AIMSink.READ_MODE.KEY_FOR_HEADER) {
      switch (field_info.type) {
        case AIMSink.FIELD_TYPE.KEY_OR_UINT32:
          if (this.bpsize < 5) return false;

          this._next_readmode();
          this._set_data_value(viewer.getUint32(this._reading_offset + 1));
          this._reading_offset += 5;
          break;
        case AIMSink.FIELD_TYPE.KEY_OR_UTF8:
          if (this.bpsize < 5) return false;

          this.string_byte_length = viewer.getUint32(this._reading_offset + 1);
          this._reading_offset += 5;
          break;
        case AIMSink.FIELD_TYPE.EOF:
        case AIMSink.FIELD_TYPE.EOF_JSON:
          this.readmode = AIMSink.READ_MODE.EOF;
          this._reading_offset += 1;
          break;
        case AIMSink.FIELD_TYPE.OBJECT_OR_ARR_CUSTOM_CLOSE:
          this._reading_offset += 1;
          this.payload.exit_scope();
          this.readmode = AIMSink.READ_MODE.DATA_FOR_VALUE; // we finished reading the object
          break;
        default:
          throw new Error(
            `Invalid field type for key, tried to read ${getENUMName(
              AIMSink.FIELD_TYPE,
              field_info.type
            )}`
          );
      }
    } else {
      switch (field_info.type) {
        case AIMSink.FIELD_TYPE.INT8:
          if (this.bpsize < 5) return false;

          this._next_readmode();
          this._set_data_value(viewer.getInt8(this._reading_offset + 1));
          this._reading_offset += 2;
          break;
        case AIMSink.FIELD_TYPE.INT16:
          if (this.bpsize < 5) return false;

          this._next_readmode();
          this._set_data_value(viewer.getInt16(this._reading_offset + 1));
          this._reading_offset += 3;
          break;
        case AIMSink.FIELD_TYPE.INT32:
          if (this.bpsize < 5) return false;

          this._next_readmode();
          this._set_data_value(viewer.getInt32(this._reading_offset + 1));
          this._reading_offset += 5;
          break;
        case AIMSink.FIELD_TYPE.BOOL:
          if (this.bpsize < 2) return false;

          this._next_readmode();
          this._set_data_value(viewer.getUint8(this._reading_offset + 1) == 1);
          this._reading_offset += 2;
          break;
        case AIMSink.FIELD_TYPE.UINT8:
          if (this.bpsize < 2) return false;

          this._next_readmode();
          this._set_data_value(viewer.getUint8(this._reading_offset + 1));
          this._reading_offset += 2;
          break;
        case AIMSink.FIELD_TYPE.UINT16:
          if (this.bpsize < 2) return false;

          this._next_readmode();
          this._set_data_value(viewer.getUint16(this._reading_offset + 1));
          this._reading_offset += 3;
          break;
        case AIMSink.FIELD_TYPE.KEY_OR_UINT32:
          if (this.bpsize < 5) return false;

          this._next_readmode();
          this._set_data_value(viewer.getUint32(this._reading_offset + 1));
          this._reading_offset += 5;
          break;
        // Special case for double, because it is 8 bytes
        case AIMSink.FIELD_TYPE.FLOAT32:
          if (this.bpsize < 5) return false;

          this._next_readmode();
          this._set_data_value(viewer.getFloat32(this._reading_offset + 1));
          this._reading_offset += 5;
          break;
        case AIMSink.FIELD_TYPE.DOUBLE:
          if (this.bpsize < 9) return false;

          this._next_readmode();
          this._set_data_value(viewer.getFloat64(this._reading_offset + 1));
          this._reading_offset += 9;
          break;
        case AIMSink.FIELD_TYPE.KEY_OR_UTF8:
          if (this.bpsize < 5) return false;

          this.string_byte_length = viewer.getUint32(this._reading_offset + 1);
          this._reading_offset += 5;
          break;
        case AIMSink.FIELD_TYPE.ARR_CUSTOM:
        case AIMSink.FIELD_TYPE.ARR_UINT8:
        case AIMSink.FIELD_TYPE.ARR_BOOL:
        case AIMSink.FIELD_TYPE.ARR_FLOAT32:
        case AIMSink.FIELD_TYPE.ARR_DOUBLE:
          if (this.bpsize < 5) return false;

          const length = viewer.getUint32(this._reading_offset + 1);
          const field = new AIMSink.FIELD_ARR_CONSTRUCTOR[field_info.type](
            length
          );
          this.payload.enter_scope(field_info.last_key, field);
          this.payload.data.metadata.length = length;
          this.payload.data.metadata.type = field_info.type;
          this.payload.data.metadata.last_key = 0;
          this._reading_offset += 5;
          break;
        case AIMSink.FIELD_TYPE.OBJECT:
          this.payload.enter_scope(field_info.last_key, {});
          this._reading_offset += 1;
          break;
        case AIMSink.FIELD_TYPE.OBJECT_OR_ARR_CUSTOM_CLOSE:
          this._reading_offset += 1;
          break;
        default:
          throw new Error("Invalid field type for value");
      }
    }
    if (this.payload.data.metadata.type === AIMSink.FIELD_TYPE.ARR_CUSTOM) {
      this.readmode = AIMSink.READ_MODE.DATA_FOR_VALUE;
    }
    this.string_buffer = "";
    this._next_readmode();
    this.payload.data.metadata.process_field_type();
    return true;
  }

  /**
   * @returns {number} bytes to read
   */
  _read_bytes_by_populated_size() {
    switch (this.payload.data.metadata.type) {
      case AIMSink.FIELD_TYPE.KEY_OR_UTF8:
        return this.bpsize < this.string_byte_length
          ? this.bpsize
          : this.string_byte_length;
      default:
        const field_info = this.payload.data.metadata;
        if (this.bpsize < field_info.byte_length) {
          const buffer_can_alloc =
            this.bpsize - (this.bpsize % field_info.value_byte_size);
          return buffer_can_alloc;
        } else {
          return field_info.byte_length;
        }
    }
  }

  _set_data_value(value) {
    const field_info = this.payload.data.metadata;
    switch (this.readmode) {
      case AIMSink.READ_MODE.DATA_FOR_HEADER:
        field_info.last_key = value;
        return;
      case AIMSink.READ_MODE.DATA_FOR_VALUE:
        this.payload.set_field(field_info.last_key, value);
        if (field_info.mode === AIMSink.FIELD_MODE.ARRAY) {
          field_info.last_key++;
        }
        return;
      default:
        throw new Error("Invalid read mode");
    }
  }

  /**
   * @param {number} bytes_to_read
   * @param {FieldMetadata} field_info
   * @returns {number} `READ_DATA_RETURN_FLAGS` should we keep reading from current buffer
   */
  _read_data(bytes_to_read, field_info) {
    const field_type = field_info.type;
    const field_length = field_info.length;
    switch (field_type) {
      case AIMSink.FIELD_TYPE.KEY_OR_UTF8:
        const section = this.bytes.subarray(
          this._reading_offset,
          this._reading_offset + bytes_to_read
        );
        if (this.string_byte_length > bytes_to_read) {
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
        const field = this.payload.data.value;
        const byteReader = AIMSink.FIELD_ARR_READER_FN[field_type];
        // FIXME: This should never happen
        if (!byteReader) {
          throw new Error("Invalid field type");
        }
        const read_bytes_to_here = this._reading_offset + bytes_to_read;
        let i_bytes = this._reading_offset;
        let i = field.length - field_info.length;
        const args = {
          arr: field,
          dataview: this.viewer,
          i,
          i_bytes,
          bytes: this.bytes,
          field_length,
        };
        while (i_bytes < read_bytes_to_here) {
          byteReader(args);
          args.i_bytes = i_bytes += field_info.value_byte_size;
          args.i = ++i;
        }
        break;
      default:
        throw new Error("Invalid field type");
    }
    return AIMSink.READ_DATA_RETURN_FLAGS.CONTINUE_READING;
  }

  _close_array_field() {
    const field_info = this.payload.data.metadata;
    if (AIMSink.FIELD_ARR_CLOSER.hasOwnProperty(field_info.type)) {
      const field = this.payload.get_field(field_info.last_key);
      this.payload.set_field(
        field_info.last_key,
        AIMSink.FIELD_ARR_CLOSER[field_info.type](field)
      );
    }
    if (field_info.mode === AIMSink.FIELD_MODE.ARRAY) {
      this.payload.exit_scope();
    }
  }

  _read_eof() {
    switch (this.payload.data.metadata.type) {
      case AIMSink.FIELD_TYPE.EOF:
        // Maybe reset so we can read another object
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
    const field_info = this.payload.data.metadata;
    readmode_switch: switch (this.readmode) {
      case AIMSink.READ_MODE.MAGIC:
        if (!this._read_stream_header()) {
          return false;
        }
        break;
      case AIMSink.READ_MODE.KEY_FOR_HEADER:
      case AIMSink.READ_MODE.KEY_FOR_VALUE:
        if (!this._read_header()) {
          return false;
        }
        break;
      case AIMSink.READ_MODE.DATA_FOR_HEADER:
      case AIMSink.READ_MODE.DATA_FOR_VALUE:
        const bytes_to_read = this._read_bytes_by_populated_size();
        switch (this._read_data(bytes_to_read, field_info)) {
          case AIMSink.READ_DATA_RETURN_FLAGS.READ_INPLACE:
            this._next_readmode();
            break readmode_switch;
          case AIMSink.READ_DATA_RETURN_FLAGS.CONTINUE_READING:
            this._reading_offset += bytes_to_read;
            if (
              this.payload.data.metadata.type === AIMSink.FIELD_TYPE.KEY_OR_UTF8
            ) {
              this.string_byte_length -= bytes_to_read;
              if (this.string_byte_length <= 0) {
                this._next_readmode();
              }
              break readmode_switch;
            }
            field_info.byte_length -= bytes_to_read;
            field_info.length -= bytes_to_read / field_info.value_size_in_bytes;
            if (field_info.length <= 0) {
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
    // if (_reading_position === this._reading_offset) {
    //   return false;
    // }

    // Remove delta from buffer_populated_size
    this.bpsize -= this._reading_offset - _reading_position;

    // Do we have more data to read?, if yes, continue reading, else return false
    return this.bpsize > 16; //threshold
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
      if (this.payload.data.metadata.type === AIMSink.FIELD_TYPE.EOF_JSON) {
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
    console.log("Took:", parseProcessEnd - parseProcess);
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
    const control = new AbortController();
    setTimeout(() => control.abort(), 10000);
    sink.write(buff, control);
    sink.close();
    const parseProcessEnd = performance.now();
    performance.measure(`binary - parseProcess`, {
      start: parseProcess,
      end: parseProcessEnd,
    });
    console.log("Took:", parseProcessEnd - parseProcess);
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
