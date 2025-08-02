import { Buffer } from 'buffer';
import { EventEmitter } from 'events';
import net from 'net';
import tls from 'tls';
import IPC from './ipc';

export class JConnection extends EventEmitter {
  /**
 * @constructs socketArgs
 * @param  {Object}    socketArgs
 * @param  {string}    [socketArgs.host]
 * @param  {number}    socketArgs.port
 * @param  {string}    [socketArgs.user]
 * @param  {string}    [socketArgs.password]
 * @param  {boolean}   [socketArgs.useBigInt]
 * @param  {boolean}   [socketArgs.enableTLS]
 * @param  {boolean}   [socketArgs.socketTimeout]
 * @param  {boolean}   [socketArgs.socketNoDelay]
 * @param  {boolean}   [socketArgs.includeNanosecond]
 * @param  {boolean}   [socketArgs.dateToMillisecond]
 */
  constructor(socketArgs) {
    super();
    this.socketArgs = socketArgs;
    this.host = socketArgs.host ?? 'localhost';
    this.port = socketArgs.port;
    this.user = socketArgs.user ?? '';
    this.password = socketArgs.password ?? '';
    this.useBigInt = socketArgs.useBigInt ?? false;
    /** @type {net.Socket|tls.TLSSocket|null} */
    this.socket = null;
    /** @type {function[]} */
    this.callbacks = [];
    this.socketTimeout = socketArgs.socketTimeout ?? 0;
    this.socketNoDelay = socketArgs.socketNoDelay ?? true;
    this.msgBuffer = Buffer.alloc(0);
    this.msgLength = 0;
    this.enableTLS = socketArgs.enableTLS ?? false;
    this.includeNanosecond = socketArgs.includeNanosecond ?? false;
    this.dateToMillisecond = socketArgs.dateToMillisecond ?? false;
  }

  setSocket(socket) {
    this.socket = socket;
    this.socket.setNoDelay(this.socketNoDelay);
    this.socket.setTimeout(this.socketTimeout);
    this.socket.on('end', () => this.emit('end'));
    this.socket.on('timeout', () => this.emit('timeout'));
    this.socket.on('error', err => this.emit('error', err));
    this.socket.on('close', err => this.emit('close', err));
    this.socket.on('data', buffer => this.incomingMsgHandler(buffer));
  }

  auth(socket, callback) {
    const userPw = `${this.user}:${this.password}`;
    const n = Buffer.byteLength(userPw, 'ascii');
    const b = Buffer.alloc(n + 2);
    b.write(userPw, 0, n, 'ascii');
    b.writeUInt8(0x9, n);
    b.writeUInt8(0x0, n + 1);
    socket.write(b);
    socket.once('data', (buffer) => {
      if (buffer.length === 1) {
        if (buffer[0] >= 9) {
          socket.removeAllListeners('close');
          socket.removeAllListeners('error');
          // reset callbacks
          this.callbacks = [];
          this.setSocket(socket);
          callback(null);
          // send error to all existing callbacks
          socket.on('close', () => {
            this.callbacks.forEach(cb => cb(new Error('LOST_CONNECTION'), null));
            this.callbacks = [];
          });
        } else {
          callback(new Error('UNSUPPORTED_IPC_VERSION<=' + buffer[0]));
        }
      } else {
        callback(new Error('INVALID_AUTH_RESPONSE'));
      }
    });
  }

  /**
   *
   * @callback errorHandler
   * @param {Error} err
   */

  /**
   *
   * @param {errorHandler} callback
   */
  connect(callback) {
    if (this.user === '') {
      this.user = process.env.USER;
    }
    if (this.socket) {
      this.socket.end();
    }
    let socket;
    const connectListener = () => {
      // won't hit connection refused, remove error listener
      socket.removeAllListeners('error');
      socket.once('close', () => {
        socket.end();
        callback(new Error('ERR_CONNECTION_CLOSED - Wrong Credentials?'));
      });
      // connection reset by peer
      socket.once('error', err => {
        socket.end();
        callback(err);
      });
      this.auth(socket, callback);
    };

    if (this.enableTLS) {
      socket = tls.connect(this.port, this.host, { rejectUnauthorized: false }, connectListener);
    } else {
      socket = net.connect(this.port, this.host, connectListener);
    }
    // connection refused
    socket.once('error', err => callback(err));
  }

  /**
   *
   * @param {function()} [callback]
   */
  close(callback) {
    this.socket.once('close', () => { if (callback) callback(); });
    this.socket.end();
  }

  /**
   *
   * @param {Buffer} buffer
   */
  incomingMsgHandler(buffer) {
    if (this.msgBuffer.length > 16) {
      // append to existing buffer, msgBuffer is allocated to the full length of the message
      buffer.copy(this.msgBuffer, this.msgLength);
      this.msgLength += buffer.length;
    } else if (this.msgBuffer.length === 0 && buffer.length >= 16) {
      const length = 16 + Number(buffer.readBigUInt64LE(8));
      if (length > buffer.length) {
        this.msgBuffer = Buffer.alloc(length);
        buffer.copy(this.msgBuffer);
        this.msgLength = buffer.length;
        // there is not enough data for deserialization
        return;
      } else {
        // length <= buffer.length
        this.msgBuffer = buffer.subarray(0, length);
        this.msgLength = buffer.length;
      }
    } else if (this.msgBuffer.length + buffer.length >= 16) {
      this.msgLength = this.msgBuffer.length + buffer.length;
      const header = Buffer.alloc(16);
      this.msgBuffer.copy(header);
      const originalLength = this.msgBuffer.length;
      buffer.copy(header, originalLength);
      const length = 16 + Number(header.readBigUInt64LE(8));
      this.msgBuffer = Buffer.alloc(length);
      header.copy(this.msgBuffer);
      buffer.copy(this.msgBuffer, 16 - originalLength);
    } else {
      // overall length < 16
      const buf = Buffer.alloc(this.msgBuffer.length + buffer.length);
      this.msgBuffer.copy(buf);
      buffer.copy(buf, this.msgBuffer.length);
      this.msgBuffer = buf;
      this.msgLength = buf.length;
    }

    // console.log(this.msgBuffer.length, this.msgLength);
    // console.log(this.msgBuffer);
    // console.log(buffer);

    if (this.msgBuffer.length > 16 && this.msgLength >= this.msgBuffer.length) {
      let obj, err;
      try {
        obj = IPC.deserialize(this.msgBuffer, this.useBigInt, this.includeNanosecond);
        err = null;
      } catch (e) {
        obj = null;
        err = e;
      }
      if (this.msgBuffer.readUInt8(1) === 2) {
        // response(2) msg
        this.callbacks.shift()(err, obj);
      } else if (this.msgBuffer.readUInt8(1) === 0) {
        // async msg(0), no need ack
        if (!err && Array.isArray(obj) && obj[0] === 'upd') {
          this.emit('upd', obj);
        }
      } else {
        // disregard sync msg(1), as this is not a q process
        // ack msg
        this.socket.write(IPC.ACK);
      }

      if (this.msgLength > this.msgBuffer.length) {
        const subBuf = buffer.subarray(buffer.length + this.msgBuffer.length - this.msgLength);
        if (subBuf.length >= 16) {
          const length = 16 + Number(subBuf.readBigUInt64LE(8));
          if (length > subBuf.length) {
            const buf = Buffer.alloc(length);
            subBuf.copy(buf);
            this.msgBuffer = buf;
          } else {
            this.msgBuffer = subBuf;
          }
        } else {
          this.msgBuffer = subBuf;
        }
        this.msgLength = subBuf.length;
      } else {
        this.msgBuffer = Buffer.alloc(0);
        this.msgLength = 0;
      }
    }
  }

  /**
   *
   * @callback queryHandler
   * @param {Error} err
   * @param {any} res
   */

  /**
   *
   * @param {string|Array} param
   * @param {queryHandler} callback
   */
  sync(param, callback) {
    if (typeof callback !== 'function') {
      throw new Error('Expecting a callback function as last param');
    }

    // null or empty list of param
    if (!param || (Array.isArray(param) && param.length === 0)) {
      this.callbacks.push(callback);
    } else {
      const buffers = IPC.serialize(param);
      let length = buffers.reduce((acc, curr) => acc + curr.length, 0);
      let header = Buffer.alloc(16);
      header.writeUInt8(0x1, 0);
      header.writeUInt8(0x1, 1);
      header.writeBigUInt64LE(BigInt(length), 8);
      this.callbacks.push(callback);
      try {
        this.socket.write(header);
        let allWritten = buffers.map(buffer => this.socket.write(buffer)).every(Boolean);
        if (!allWritten) {
          this.callbacks.pop()(new Error('LOST_CONNECTION'), null);
        }
      } catch (e) {
        this.callbacks.pop()(e, null);
      }
    }
  }

  /**
   *
   * @param {string|Array} param
   * @param {errorHandler} [callback]
   */
  asyn(param, callback) {
    const buffers = IPC.serialize(param);
    let length = buffers.reduce((acc, curr) => acc + curr.length, 0);
    let header = Buffer.alloc(16);
    header.writeUInt8(0x1, 0);
    header.writeBigUInt64LE(BigInt(length), 8);
    let lastBuffer = buffers.pop();
    this.socket.write(header);
    buffers.forEach(buffer => this.socket.write(buffer));
    if (callback) {
      this.socket.write(lastBuffer, callback);
    } else {
      this.socket.write(lastBuffer);
    }
  }
}
