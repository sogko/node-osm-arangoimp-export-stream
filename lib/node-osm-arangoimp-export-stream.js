var util = require('util');
var fs = require('fs');
var path = require('path');
var Transform = require('stream').Transform;
var OSMStream = require('node-osm-stream');
var EOL = require('os').EOL;
var async = require('async');
var mkdirp = require('mkdirp');
var level = require('levelup');
var logger = require('./logger')('[OSMArangoImpExportStream]').log;
logger = function () {}; // comment out to show debug logs

var _uniq = function (arr) {
  return arr.slice().sort(function(a,b){return a - b}).reduce(function(a,b){if (a.slice(-1)[0] !== b) a.push(b);return a;},[]);
};

function OSMArangoImpExportStream(options) {
  if (!(this instanceof OSMArangoImpExportStream)) return new OSMArangoImpExportStream(options);

  Transform.call(this);

  this.mode = (!options.mode) ? 'arangoimp' : (['arangoimp', 'json'].indexOf(options.mode.toString().toLowerCase()) > -1) ? options.mode.toString().toLowerCase() : 'arangoimp';
  this.destination = options.destination || null;
  this.keepCache = options.keepCache || false;

  // ensure we have set of default collections
  options.collections = options.collections || [];
  if (!options.collections instanceof Array) options.collections = [options.collections];
  this.collections = _uniq(['nodes', 'ways', 'relations'].concat(options.collections||[]));

  logger('Options:', 'mode:', this.mode, 'destination:', this.destination, 'keepCache:', this.keepCache, 'collections:', this.collections);

  // size of bytes read from source so far
  this.bytesRead = 0;
  // size of bytes written to stream so far
  this.bytesWritten = 0;

  this._hasWrittenFirstLine = {
    pipe: false,
    collections: {}
  };
  this._hasEmittedStart = false;
  this._rstream = null;
  this._wstream = {};
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
      this._createCollectionStreams();
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
  // usually the reference is needed for common attributes such as lat/long and timestamp
  // the data here might be re-written.
  // so when reading from cache, there is no guarantee it will return the transformed data or the original data.
  this._writeToCache(object);

  var emitForUserTransform = function (type, object, relatedObjects) {

    // after receiving transformed object by user from callback, write it to relevant pipes/cache
    var writeUserTransformObject = function (collection, object) {
      if (!collection || !object) return;

      if (['node', 'way', 'relation'].indexOf(collection) > -1) collection = collection +'s';
      if (this.collections.indexOf(collection) < 0) {
        collection = null;
      }

      if (!(object instanceof Array)) object = [object];
      object.forEach(function(o){
        if (!o) return;
        this.__push(collection, o);  // write to outgoing stream
        this._writeToCache(o); // write to cache
      }.bind(this));
    }.bind(this);

    // primary wrapper to handle callback from user
    var handleUserTransformCallback = function () {
      // ie user returned empty callback
      if (arguments.length === 0) return callback();

      // ie. function(object){}
      if (arguments.length === 1) {
        var collection = type; // scope: emitForUserTransform()
        writeUserTransformObject(collection, arguments[0]);
        return callback();
      }

      // else function(collectionName1, object1, collectionName2, object2, ...) {};
      for (var i = 0; i < arguments.length; i+=2) {
        collection = arguments[i];
        object = (i+1 < arguments.length) ? arguments[i+1] : null;
        writeUserTransformObject(collection, object);
      }
      return callback();
    }.bind(this);

    if (this.listeners(type).length === 0) {
      return handleUserTransformCallback(object);
    }

    if (relatedObjects) {
      return this.emit(type, object, relatedObjects, handleUserTransformCallback);
    }
    return this.emit(type, object, handleUserTransformCallback);

  }.bind(this);

  var handleObjectTypeNode = function (object) {
    emitForUserTransform('node', object, null);
  }.bind(this);

  var handleObjectTypeWay = function (object) {

    // get referenced nodes' waypoints in lat/lng and store it per way
    var polyline = [];
    // store and pass the node objects to let consumer further transform this if needed
    var nodes = {};

    async.each(object.nodes, function (node_id, next) {

      this._getFromCache(node_id, 'node', function (err, node_id, node) {
        // it is possible that ways refers to nodes that are not within an extract due to bounds
        if (!node) return next();

        if (node && node.lat && node.lon) polyline.push([node.lat, node.lon]);
        nodes[node.id] = node;
        next();
      }.bind(this));

    }.bind(this), function (err) {
      if (err) this.emit('error', err);

      object.polyline = polyline;

      emitForUserTransform('way', object, nodes);

    }.bind(this));

  }.bind(this);

  var handleObjectTypeRelation = function (object) {

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

      emitForUserTransform('relation', object, members);

    }.bind(this));

  }.bind(this);

  if (object.type === 'node') {
    handleObjectTypeNode(object);
  } else if (object.type === 'way') {
    handleObjectTypeWay(object);
  } else if (object.type === 'relation') {
    handleObjectTypeRelation(object);
  }

};

OSMArangoImpExportStream.prototype._flush = function (callback) {

  logger('_flush');
  this.__push(); // signals EOF to outgoing stream

  this._closeCollectionStreams();
  this._closeCaches(function () {
    callback();
  }.bind(this));

};

OSMArangoImpExportStream.prototype.open = function (file_path) {
  this._rstream = fs.createReadStream(file_path);
  this._rstream.pipe(this);
};

OSMArangoImpExportStream.prototype.__push = function (collection, object) {

  // handle the case when EOF signal is written to close outgoing stream (end-of-file)
  // __push(null|EOF)
  if (typeof object === 'undefined' || object === null) {
    // send EOF signals to primary outgoing pipe and collection streams
    this.__pushToPipe(null);
    for (var i in this.collections) {
      this.__pushToCollection(this.collections[i], null);
    }
    this.push(null);
    logger('__push: sent out final data to outgoing stream');
    return;
  }

  logger('_push', object);

  // stringify JSON object
  if (object instanceof Object) {
    object = JSON.stringify(object);
  }

  this.__pushToPipe(object);
  this.__pushToCollection(collection, object);

  logger('_push this.bytesWritten', this.bytesWritten);

};

OSMArangoImpExportStream.prototype.__pushToPipe = function (str) {

  var _countAndPush = function (str) {
    var buff = new Buffer(str);
    this.push(buff);
    this.bytesWritten += buff.length;
  }.bind(this);

  // handle first data case
  if (!this._hasWrittenFirstLine.pipe) {
    this._hasWrittenFirstLine.pipe = true;
    if (this.mode === 'json') {
      // add an opening square bracket before the first JSON object
      // that we are about to write to file
      _countAndPush(['[', EOL, '  ', str].join(''));
    }
  }

  // handle last data case: when EOF signal is written to close outgoing stream (end-of-file)
  // __pushToPipe(null|EOF);
  if (typeof str === 'undefined' || str === null) {
    if (this.mode === 'json') {
      _countAndPush([EOL, ']', EOL].join(''));
    }
    this.push(null);
    logger('__push: sent out final data to outgoing stream');
    return;
  }

  // handle new data (not the first/last)
  if (this.mode === 'json') {
    // prepend a comma to the rest of the JSON objects
    str = [',', EOL, '  ', str].join('');
  } else if (this.mode === 'arangoimp') {
    str = [str, EOL].join('');
  }

  // write to outgoing pipe
  _countAndPush(str);

};

OSMArangoImpExportStream.prototype.__pushToCollection = function (collection, str) {
  if (!this._wstream[collection]) return;

  // handle first data case
  if (!this._hasWrittenFirstLine.collections[collection]) {
    this._hasWrittenFirstLine.collections[collection] = true;
    if (this.mode === 'json') {
      // add an opening square bracket before the first JSON object
      // that we are about to write to file
      this._wstream[collection].write(['[', EOL, '  ', str].join(''));
    }
  }

  // handle last data case: __pushToCollection(null|EOF)
  if (typeof str === 'undefined' || str === null) {
    if (this.mode === 'json') {
      this._wstream[collection].write([EOL, ']', EOL].join(''));
    }
    this._wstream[collection].end();
    return;
  }

  // handle new data (not the first/last)
  if (this.mode === 'json') {
    // prepend a comma to the rest of the JSON objects
    str = [',', EOL, '  ', str].join('');
  } else if (this.mode === 'arangoimp') {
    str = [str, EOL].join('');
  }

  this._wstream[collection].write(str); // write to collection file

};

OSMArangoImpExportStream.prototype._createCollectionStreams = function () {

  logger('_createCollectionStreams Writing collections to : ', this.destination);
  mkdirp.sync(this.destination);

  async.eachSeries(this.collections, function(collection, next) {
    var collection_path = path.join(this.destination, collection + '.json');
    this._wstream[collection] = fs.createWriteStream(collection_path);
    this._wstream[collection].once('open', function(){ next(); });
  }.bind(this));

  this.pipe(fs.createWriteStream(this.destination + '/all.json'));
};

OSMArangoImpExportStream.prototype._closeCollectionStreams = function () {
  logger('_closeCollectionStreams');

  async.eachSeries(this.collections, function(collection, next) {
    this._wstream[collection].end(function(){
      next()
    });
  }.bind(this));
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