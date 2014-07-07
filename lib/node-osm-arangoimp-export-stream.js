var util = require('util');
var fs = require('fs');
var Transform = require('stream').Transform;
var OSMStream = require('node-osm-stream');
var EOL = require('os').EOL;
var logger = require('./logger')('[OSMArangoImpExportStream]').log;
logger = function(){}; // comment out to show debug logs

function OSMArangoImpExportStream(options) {
  if (!(this instanceof OSMArangoImpExportStream)) return new OSMArangoImpExportStream(options);

  Transform.call(this);

  this.mode = (!options.mode) ? 'arangoimp' : (['arangoimp', 'json'].indexOf(options.mode.toLowerCase()) > -1) ? options.mode.toLowerCase() : 'arangoimp';
  this.destination = options.destination || null;

  logger('Options:', this.mode, this.destination);

  // size of bytes read from source so far
  this.bytesRead = 0;
  // size of bytes written to stream so far
  this.bytesWritten = 0;

  this._hasWrittenFirstLine = false;
  this._hasEmittedStart = false;
  this._rstream = null;
  this._wstream = null;
  this._parser = OSMStream();

  // Pipeline
  // ========
  //                              |                                           |
  //    (external stream) ====>   |  ====> (this._parser) ===> (this) ====>   |   ====>   (outgoing stream)
  //                              |                                           |
  //
  // When receiving a pipe from an external source (either by src.pipe(this) or this.open(file_path) ),
  // redirect the incoming pipe to this._parser (inherited from stream.Transform class).
  // Next, pipe outgoing stream from this._parser back to this object incoming stream.
  // Any incoming stream will then be simply passed out to this object outgoing stream.
  //
  // If a destination file path was specified, we will help to create a fs.WriteStream.
  // A consumer can still take this object's outgoing stream to anything that wants to read it.

  // prepare to handle pipe event from a source
  this.on('pipe', (function (src) {
    logger('Received pipe from:', src.path || src.constructor.name || '<unindentified pipe>');
    if (src === this._parser) return;

    logger('Unhook received pipe and re-pipe to _parser');
    src.unpipe(this);
    src.pipe(this._parser);

    logger('Pipe _parser outgoing stream back to our incoming stream');
    this._parser.pipe(this);

    if (this.destination) {
      logger('Writing file out to: ', this.destination);
      this._wstream = fs.createWriteStream(this.destination);
      this.pipe(this._wstream);
    }

    src.on('data', (function (chunk) {
      this.bytesRead += chunk.length;
      this.emit('incoming_data', chunk);
    }).bind(this));
  }).bind(this));

  this.on('data', (function () {
    if (!this._hasEmittedStart) {
      this._hasEmittedStart = true;
      this.emit('start');
    }
  }).bind(this));

  // handle events for our osm parser
  var onElement = function (name, object, callback) {
    logger('_parser.on ', name);

    if (this.listeners(name).length > 0) {
      this.emit(name, object, (function (obj) {
        logger('_parser.on cb 1', name);
        callback(obj);
      }).bind(this));
    } else {
      logger('_parser.on  cb 2', name);

      callback(object);
    }
  }.bind(this);

  this._parser.on('node', (function (node, callback) {
    onElement('node', node, callback);
  }).bind(this));

  this._parser.on('way', (function (way, callback) {
    onElement('way', way, callback);
  }).bind(this));

  this._parser.on('relation', (function (relation, callback) {
    onElement('relation', relation, callback);
  }).bind(this));

  this._parser.on('writeable', (function (data, callback) {
    // parser has data ready to write to outgoing stream
    logger('_parser writeable');

    if (this.mode === 'json') {
      if (!this._hasWrittenFirstLine) {
        this._hasWrittenFirstLine = true;
        // add an opening square bracket before the first JSON object
        // that we are about to write to file
        data = ['[', EOL, '  ', JSON.stringify(data)].join('');
      } else {
        // prepend a comma to the rest of the JSON objects
        data = [',', EOL, '  ', JSON.stringify(data)].join('');
      }
    }

    // we take a copy of the data and write it to our outgoing stream
    this.__push(data);

    // pass back to _parser to allow it to write out to its own outgoing stream
    callback(data);

  }).bind(this));

  this._parser.on('flush', (function (callback) {

    var lastData = this.__getDataToFlush();
    logger('_parser onFlush: ', lastData);
    callback(lastData);

  }).bind(this));

  this._parser.on('finish', (function (data, callback) {

  }).bind(this));

  this._parser.on('end', (function (data, callback) {

  }).bind(this));

  this._parser.on('close', (function (data, callback) {

  }).bind(this));

  this._parser.on('error', (function (error) {
    logger('_parser error', error);
    throw new Error(error);
  }).bind(this));

  this.on('error', (function (error) {
    logger('error', error);
    throw new Error(error);
  }).bind(this));
}

util.inherits(OSMArangoImpExportStream, Transform);

OSMArangoImpExportStream.prototype._transform = function (chunk, enc, callback) {
  logger('_transform', chunk.length);
  // Let data pass through here, transformation and
  // writing to stream will be done by this._parser.
  callback();
};

OSMArangoImpExportStream.prototype._flush = function (callback) {
  var lastData = this.__getDataToFlush();
  logger('_flush: ', lastData);
  this.__push(lastData);
  callback();
};

OSMArangoImpExportStream.prototype.open = function (file_path) {
  this._rstream = fs.createReadStream(file_path);
  this._rstream.pipe(this);
};

OSMArangoImpExportStream.prototype.__push = function (object) {
  if (object) {
    logger('__push');
    var buff = new Buffer(object);
    this.push(buff);
    this.bytesWritten += buff.length;

  }
};

OSMArangoImpExportStream.prototype.__getDataToFlush = function () {
  var lastData = EOL;
  if (this.mode === 'json') {
    lastData = [EOL,']', EOL].join('');
  }
  return lastData;
};
module.exports = OSMArangoImpExportStream;