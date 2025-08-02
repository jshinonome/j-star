import { RecordBatchStreamReader, Table, tableToIPC } from 'apache-arrow';


const MS_PER_DAY = 86400000;
const PADDING = [[], [0, 0, 0, 0, 0, 0, 0], [0, 0, 0, 0, 0, 0], [0, 0, 0, 0, 0], [0, 0, 0, 0], [0, 0, 0], [0, 0], [0]];
/**
 *
 * @param {BigInt} ns
 * @returns {string}
 */
function bigintToDuration(ns) {
  const sign = ns < 0n ? '-' : '';
  if (ns < 0n) {
    ns = -1n * ns;
  }
  const second = ns / 1000000000n;
  const days = ns / 86400000000000n;
  const hh = second / 3600n % 24n;
  const mm = second / 60n % 60n;
  const ss = second % 60n;
  const SSS = ns % 1000000000n;
  return `${sign}${days}D${String(hh).padStart(2, '0')}:${String(mm).padStart(2, '0')}:${String(ss).padStart(2, '0')}.${String(SSS).padStart(9, '0')}`;
}

/**
 *
 * @param {BigInt} ns
 * @returns {string}
 */
function bigIntToTime(ns) {
  let hh, mm, ss, SSS;
  const seconds = ns / 1000000000n;
  hh = seconds / 3600n;
  mm = seconds / 60n % 60n;
  ss = seconds % 60n;
  SSS = ns % 1000000000n;
  return [hh, mm, ss].map(val => String(val).padStart(2, '0')).join(':') + '.' + String(SSS).padStart(9, '0');
}

/**
 *
 * @param {Buffer} buffer
 * @param {boolean} useBigInt
 * @param {boolean} includeNanosecond
 * @returns {any}
 */
function deserialize(buffer, useBigInt = false, includeNanosecond = false) {
  let offset = 16;
  const read = () => {
    let jType = buffer[offset];
    offset += 4;
    switch (jType) {
      case 255: {
        // boolean
        const bool = buffer[offset] === 1;
        offset += 4;
        return bool;
      }
      case 254: {
        const u8 = buffer[offset];
        offset += 4;
        return u8;
      }
      case 253: {
        const i16 = buffer.readInt16LE(offset);
        offset += 4;
        return i16;
      }
      case 252:
      case 250: {
        // short
        const i32 = buffer.readInt32LE(offset);
        offset += 4;
        if (jType === 252) {
          return i32;
        } else {
          return new Date(i32 * MS_PER_DAY);
        }
      }
      case 251:
      case 249:
      case 248:
      case 247:
      case 246: {
        offset += 4;
        const bigI64 = buffer.readBigInt64LE(offset);
        offset += 8;
        if (jType === 251) {
          return useBigInt ? bigI64 : Number(bigI64);
        } else if (jType === 249) {
          return bigIntToTime(bigI64);
        } else if (jType === 248) {
          return new Date(Number(bigI64));
        } else if (jType === 247) {
          let timestamp = new Date(Number(bigI64 / 1000000n));
          if (includeNanosecond) {
            return timestamp.toISOString().slice(0, -1) + String(bigI64 % 1000000n).padStart(6, '0');
          } else {
            return timestamp;
          }
        } else {
          return bigintToDuration(bigI64);
        }
      }
      case 245: {
        let f32 = buffer.readFloatLE(offset);
        offset += 4;
        return f32;
      }
      case 244: {
        offset += 4;
        let f64 = buffer.readDoubleLE(offset);
        offset += 8;
        return f64;
      }
      case 0: {
        offset += 4;
        return null;
      }
      case 243:
      case 242:
      case 128:
      case 154: {
        let byteLen = buffer.readUInt32LE(offset);
        offset += 4;
        let s = buffer.subarray(offset, offset + byteLen).toString('utf8');
        offset += byteLen + PADDING[byteLen % 8].length;
        if (jType === 128) {
          return new Error(s);
        }
        return s;
      }
      case 90: {
        let listLen = buffer.readUInt32LE(offset);
        offset += 4;
        // skip list full length
        offset += 8;
        const array = new Array(listLen);
        for (let i = 0; i < listLen; i++) {
          array[i] = read();
        }
        return array;
      }
      case 92: {
        offset += 4;
        let byteLen = Number(buffer.readBigUInt64LE(offset));
        offset += 8;
        const reader = RecordBatchStreamReader.from(buffer.subarray(offset, offset + byteLen));
        const table = new Table(reader.readAll());
        offset += byteLen;
        return table;
      }
      // one column table
      case jType >= 1 && jType <= 19: {
        offset += 4;
        let byteLen = Number(buffer.readBigUInt64LE(offset));
        offset += 8;
        const reader = RecordBatchStreamReader.from(buffer.subarray(offset, offset + byteLen));
        const table = new Table(reader.readAll());
        offset += byteLen;
        return table;
      }
      case 91: {
        let dictLen = buffer.readUInt32LE(offset);
        offset += 4;
        const map = new Map();
        if (dictLen === 0) {
          return map;
        }
        // let _len = Number(buffer.readBigUInt64LE(offset));
        offset += 8;
        let keyLen = Number(buffer.readBigUInt64LE(offset));
        offset += 8;

        let offsets = new Uint32Array(buffer.buffer, buffer.byteOffset + offset, dictLen);
        let keys = buffer.subarray(offset + 4 * dictLen, offset + keyLen);
        offset += keyLen + PADDING[keyLen % 8].length;

        // let valueLen = Number(buffer.readBigUInt64LE(offset));
        offset += 8;
        // let values = buffer.subarray(offset, offset + valueLen);
        // offset += valueLen + PADDING[valueLen % 8].length;

        let prevOffset = 0;
        for (let i = 0; i < dictLen; i++) {
          let currentOffset = offsets[i];
          let key = keys.subarray(prevOffset, currentOffset).toString('utf8');
          prevOffset = currentOffset;
          let value = read();
          map.set(key, value);
        }

        return map;
      }
      default: {
        throw new Error('Unsupported jType: ' + jType);
      }
    }
  };
  return read();
}

/**
 *
 * @param {any} obj
 * @returns {Array<Buffer>}
 */
function serialize(obj) {
  let buffers = [];
  if (obj instanceof Table) {
    const tableBuf = tableToIPC(obj, "stream");
    let header = Buffer.alloc(16);
    header.writeUInt8(92, 0);
    header.writeBigUInt64LE(BigInt(tableBuf.length), 8);
    buffers.push(header);
    buffers.push(tableBuf);
  } else if (obj instanceof Date) {
    let buffer = Buffer.alloc(16);
    const int64 = BigInt(obj.getTime());
    buffer.writeUInt8(248, 0);
    buffer.writeBigInt64LE(int64, 8);
    buffers.push(buffer);
  } else if (obj instanceof Array) {
    let arrayBuffers = [];
    for (let item of obj) {
      let itemBuffers = serialize(item);
      arrayBuffers = arrayBuffers.concat(itemBuffers);
    }
    let header = Buffer.alloc(16);
    header.writeUInt8(90, 0);
    header.writeUInt32LE(obj.length, 4);
    let length = arrayBuffers.reduce((acc, curr) => acc + curr.length, 0);
    header.writeBigUInt64LE(BigInt(length), 8);
    buffers.push(header);
    buffers = buffers.concat(arrayBuffers);
  } else if (obj === null) {
    let buffer = Buffer.alloc(8);
    return [buffer]
  } else {
    switch (typeof obj) {
      case 'number':
        if (Number.isInteger(obj)) {
          let buffer = Buffer.alloc(16);
          buffer.writeUInt8(251, 0);
          buffer.writeBigInt64LE(BigInt(obj), 8);
          return [buffer];
        } else {
          let buffer = Buffer.alloc(16);
          buffer.writeUInt8(244, 0);
          buffer.writeDoubleLE(obj, 8);
          return [buffer];
        }
      case 'boolean': {
        let buffer = Buffer.alloc(8);
        buffer.writeUInt8(255, 0);
        buffer.writeUInt8(obj ? 1 : 0, 4);
        return [buffer];
      }
      case 'string': {
        let byteLength = Buffer.byteLength(obj, 'utf8');
        let length = 8 + byteLength + PADDING[byteLength % 8].length;
        let buffer = Buffer.alloc(length);
        buffer.writeUInt8(243, 0);
        buffer.writeUInt32LE(byteLength, 4);
        buffer.write(obj, 8);
        return [buffer];
      }
      case 'bigint': {
        let buffer = Buffer.alloc(16);
        buffer.writeUInt8(251, 0);
        buffer.writeBigInt64LE(obj, 8);
        return [buffer];
      }
      // treat as a dict
      case 'object' || obj instanceof Map: {
        let entries = obj instanceof Map ? Array.from(obj) : Object.entries(obj);
        if (entries.length === 0) {
          return [Buffer.from([91, 0, 0, 0, 0, 0, 0, 0])];
        } else {
          // type(4), dict length(4), bytes length(8), keys length(8)
          let header = Buffer.alloc(24);
          header.writeUInt8(91, 0);
          header.writeUInt32LE(entries.length, 4);
          let keysLength = entries.length * 4 + entries.reduce((acc, curr) => {
            return acc + Buffer.byteLength(curr[0], 'utf8');
          }, 0);
          keysLength = keysLength + PADDING[keysLength % 8].length;
          let keysBuffer = Buffer.alloc(keysLength);
          let valueBuffers = [];
          let bytesOffset = entries.length * 4;
          let prevOffset = 0;
          for (let [index, [key, value]] of entries.entries()) {
            let keyLength = Buffer.byteLength(key, 'utf8');
            prevOffset += keyLength;
            keysBuffer.writeUint32LE(prevOffset, index * 4);
            keysBuffer.write(key, bytesOffset);
            bytesOffset += keyLength;
            valueBuffers = valueBuffers.concat(serialize(value));
          }
          let valuesLength = valueBuffers.reduce((acc, curr) => {
            return acc + curr.length;
          }, 0);
          header.writeBigUInt64LE(BigInt(keysLength + valuesLength), 8);
          header.writeBigUInt64LE(BigInt(keysLength), 16);
          buffers.push(header);
          buffers.push(keysBuffer);
          let valueLengthBuf = Buffer.alloc(8);
          valueLengthBuf.writeBigUInt64LE(BigInt(valuesLength), 0);
          buffers.push(valueLengthBuf);
          buffers = buffers.concat(valueBuffers);
        }
      }
    }
  }
  return buffers;
}

// true
const ACK = Buffer.from([1, 2, 0, 0, 0, 0, 0, 0, 8, 0, 0, 0, 0, 0, 0, 0, 255, 0, 0, 0, 1, 0, 0, 0]);

export default { deserialize, serialize, ACK };
