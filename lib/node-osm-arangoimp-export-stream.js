var util = require('util');
var fs = require('fs');
var Transform = require('stream').Transform;
var OSMStream = require('node-osm-stream');
var EOL = require('os').EOL;
var async = require('async');
var level = require('levelup');
var logger = require('./logger')('[OSMArangoImpExportStream]').log;
logger = function () {}; // comment out to show debug logs

function OSMArangoImpExportStream(options) {
  if (!(this instanceof OSMArangoImpExportStream)) return new OSMArangoImpExportStream(options);

  Transform.call(this);

  this.mode = (!options.mode) ? 'arangoimp' : (['arangoimp', 'json'].indexOf(options.mode.toString().toLowerCase()) > -1) ? options.mode.toString().toLowerCase() : 'arangoimp';
  this.destination = options.destination || null;
  this.keepCache = options.keepCache || false;

  logger('Options:', 'mode:', this.mode, 'destination:', this.destination, 'keepCache:', this.keepCache);

  // size of bytes read from source so far
  this.bytesRead = 0;
  // size of bytes written to stream so far
  this.bytesWritten = 0;

  this._hasWrittenFirstLine = false;
  this._hasEmittedStart = false;
  this._rstream = null;
  this._wstream = null;
  this._parser = OSMStream();

  // cache db
  this._cache = null;
  this._cacheName = './cache';
  this._cacheWriteStream = null;
  this._openCaches();

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

  /**
   * Notes about this._parser chain of events (node-osm-stream)
   *
   * this._parser.on('node'|'way'|'relation')
   * - when the parser received an osm object element, before writing to outgoing stream.
   * - it allows listener to modify object on callback, after which it goes to writable event
   *
   * this._parser.on('writeable')
   * - when parser has data ready to write to outgoing stream,
   *   which will be piped back to us in this._transform()
   *
   * this._parser.on('flush')
   * - parser has finished reading and parsing all data and sent all data to its outgoing stream.
   * - allows listener to push final data to outgoing stream before closing it
   *
   * this._parser.on('finish')
   * this._parser.on('end')
   * - From official stream.Transform API documentation
   * - The 'finish' event is fired after .end() is called and all chunks have been processed
   *   by _transform
   * - The 'end' is fired after all data has been output which is after
   *   the callback in _flush has been called.
   *
   */

  this._parser.on('flush', (function (callback) {
    logger('_parser flush');
    callback(); // the moment we send this, parser ends
  }).bind(this));

  this._parser.on('finish', (function () {
    logger('_parser finish');

  }).bind(this));

  this._parser.on('end', (function () {
    logger('_parser end');
  }).bind(this));

  this._parser.on('error', (function (error) {
    logger('_parser error', error);
    throw new Error(error);
  }).bind(this));


  this.on('error', (function (error) {
    logger('error', error);
    throw new Error(error);
  }).bind(this));

  this.on('finish', function () {
    logger('finish');
  });

  this.on('end', function () {
    logger('end');

  });
}

util.inherits(OSMArangoImpExportStream, Transform);

OSMArangoImpExportStream.prototype._transform = function (chunk, enc, callback) {
  // got data from parser outgoing pipe
  logger('_transform', chunk.toString());

  var object = chunk.toString();

  try {
    object = JSON.parse(object);
  } catch (e) {
    logger('_transform, failed to parse JSON object', object, e);
    throw new Error('_transform failed to parse JSON object', object);
  }

  // write pre-transformed object to cache first for others to get a reference to it.
  // usually the reference is needed for common attributes suck as lat/long and timestamp
  // the data here might be re-written.
  // so when reading from cache, there is no guarantee it will return the transformed data.
  this._writeToCache(object);

  var handlePostTransform = function (object, callback) {
    // write transformed object to cache outgoing stream and cache
    this.__push(object);
    this._writeToCache(object);
    callback();
  }.bind(this);

  var handlePostRetrievingRelatedObjects = function (type, object, relatedObjects, callback) {

    if (this.listeners(type).length === 0) return handlePostTransform(object, callback);

    if (relatedObjects) {
      return this.emit(type, object, relatedObjects, function (returnedObject) {
        handlePostTransform(returnedObject, callback);
      }.bind(this));
    }

    return this.emit(type, object, function (returnedObject) {
      handlePostTransform(returnedObject, callback);
    }.bind(this));

  }.bind(this);

  var handleObjectTypeNode = function (object, callback) {
    handlePostRetrievingRelatedObjects('node', object, null, callback);
  }.bind(this);

  var handleObjectTypeWay = function (object, callback) {

    // get referenced nodes' waypoints in lat/lng and store it per way
    var waypoints = [];
    // store and pass the node objects to let consumer further transform this if needed
    var nodes = {};

    async.each(object.nodes, function (node_id, next) {

      this._getFromCache(node_id, 'node', function (err, node_id, node) {
        // it is possible that ways refers to nodes that are not within an extract due to bounds
        if (!node) return next();

        if (node && node.lat && node.lon) waypoints.push([node.lat, node.lon]);
        nodes[node.id] = node;
        next();
      }.bind(this));

    }.bind(this), function (err) {
      if (err) this.emit('error', err);

      object.waypoints = waypoints;

      handlePostRetrievingRelatedObjects('way', object, nodes, callback);

    }.bind(this));

  }.bind(this);

  var handleObjectTypeRelation = function (object, callback) {

    // store and pass the member objects to let consumer further transform this if needed
    var members = {
      nodes: {},
      ways: {},
      relations: {}
    };

    async.each(object.members, function (member, next) {

      var object_id = member.ref;
      var object_type = member.type;
      this._getFromCache(object_id, object_type, function (err, object_id, object) {
        // it is possible that relations refers to nodes/ways that are not within an extract due to bounds
        if (!object) return next();

        // concatenating with 's' is a little bit disgusting, but its a lot better than unnecessary if-else for now
        members[object_type + 's'][object_id] = object;
        next();
      }.bind(this));

    }.bind(this), function (err) {
      if (err) this.emit('error', err);

      handlePostRetrievingRelatedObjects('relation', object, members, callback);

    }.bind(this));

  }.bind(this);

  if (object.type === 'node') {
    handleObjectTypeNode(object, callback);
  } else if (object.type === 'way') {
    handleObjectTypeWay(object, callback);
  } else if (object.type === 'relation') {
    handleObjectTypeRelation(object, callback);
  }

};

OSMArangoImpExportStream.prototype._flush = function (callback) {

  logger('_flush');
  this.__push(null); // signals EOF to outgoing stream

  this._closeCaches(function () {
    callback();
  });

};

OSMArangoImpExportStream.prototype.open = function (file_path) {
  this._rstream = fs.createReadStream(file_path);
  this._rstream.pipe(this);
};

OSMArangoImpExportStream.prototype.__push = function (object) {

  // handle the case when EOF signal is written to close outgoing stream (end-of-file)
  if (typeof object === 'undefined' || object === null) {
    if (this.mode === 'json') this.push([EOL, ']', EOL].join(''));
    this.push(null);
    logger('__push: sent out final data to outgoing stream');
    return;
  }

  logger('_push', object);

  // stringify JSON object
  if (object instanceof Object) {
    object = JSON.stringify(object);
  }

  // transform the push object further depending on the export mode
  if (this.mode === 'json') {
    if (!this._hasWrittenFirstLine) {
      this._hasWrittenFirstLine = true;
      // add an opening square bracket before the first JSON object
      // that we are about to write to file
      object = ['[', EOL, '  ', object].join('');
    } else {
      // prepend a comma to the rest of the JSON objects
      object = [',', EOL, '  ', object].join('');
    }
  } else if (this.mode === 'arangoimp') {
    object = [object, EOL].join('');
  }

  var buff = new Buffer(object);
  this.push(buff);
  this.bytesWritten += buff.length;

  logger('_push this.bytesWritten', this.bytesWritten);

};

OSMArangoImpExportStream.prototype._openCaches = function () {
  logger('_openCaches');

  level.destroy(this._cacheName, function (err) {
    logger('_openCaches removed old db');

    if (err) {
      this.emit('error', err);
      return;
    }

    this._cache = level(this._cacheName, {valueEncoding: 'json'});
    this._cacheWriteStream = this._cache.createWriteStream({
      keyEncoding: 'utf8',
      valueEncoding: 'json'
    });

    this._cacheWriteStream.on('error', function (err) {
      logger('_openCaches _cacheWriteStream, err:', err);
      if (err) {
        this.emit('error', err);
      }
    }.bind(this));
  }.bind(this));

};

OSMArangoImpExportStream.prototype._closeCaches = function (callback) {
  logger('_closeCaches');

  var handleOnCloseCacheDb = function (err) {
    logger('_cache close');
    if (err) this.emit('error', err);

    if (this.keepCache) {
      return callback();
    }

    level.destroy(this._cacheName, function (err) {
      if (err) this.emit('error', err);
      callback();
    }.bind(this));

  }.bind(this);

  var handleOnCloseWriteStream = function () {
    logger('_cacheWriteStream closed');
    this._cache.close(handleOnCloseCacheDb);
  }.bind(this);

  this._cacheWriteStream.on('close', handleOnCloseWriteStream);

  this._cacheWriteStream.end();

};

OSMArangoImpExportStream.prototype._determineCacheKey = function (id, type) {

  // http://wiki.openstreetmap.org/wiki/Objects#Common_attributes:
  // element.id:
  //    integer
  //    Used for identifying the element. Element types have their own ID space, so there could be a node with id=100
  //    and a way with id=100, which are unlikely to be related or geographically near to each other.
  //
  // http://wiki.openstreetmap.org/wiki/OSM_XML#Assumptions
  // - ids can be non-negative

  if (!id || !type) return null;
  return [type, '_', id].join('');

};

OSMArangoImpExportStream.prototype._writeToCache = function (object) {

  logger('_writeToCache', (object) ? object.id : object);

  if (!object) return;

  var key = this._determineCacheKey(object.id, object.type);
  if (!key) return;
  this._cache.put(key, object);

};
OSMArangoImpExportStream.prototype._getFromCache = function (id, type, callback) {

  logger('_getFromCache', id, type);

  if (!id || !type) return callback(new Error('Empty id/type'), null, null);

  var key = this._determineCacheKey(id, type);
  if (!key) return callback(new Error('Empty key'), null, null);

  logger('_getFromCache', key);

  this._cache.get(key, {}, function (err, data) {

    if (err && err.length) {
      logger('_getFromCache results err', err, JSON.stringify(err), util.inspect(err));
      return callback(err, null, null);
    }

    logger('_getFromCache results', key, data, err);
    callback(null, id, data);

  }.bind(this));

};
module.exports = OSMArangoImpExportStream;