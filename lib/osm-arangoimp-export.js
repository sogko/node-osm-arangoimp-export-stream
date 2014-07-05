var expat = require('node-expat');
var fs = require('fs');
var util = require('util');
var extend = util._extend;
var events = require('events');
var EOL = require('os').EOL;
var Readable = require('stream').Readable;

var logger = require('./logger')('[OSMArangoImpExport]').log;
logger = function(){}; // comment out to show debug logs

function DataStream(options) {
    Readable.call(this, options);
}
util.inherits(DataStream, Readable);
DataStream.prototype._read = function(size) {};
DataStream.prototype.queue = function(data) { this.push(data); };
DataStream.prototype.end = function() {

    var closeStream = (function () {
        this.queue(null);   // 'end' event will be emitted by Readable
    }.bind(this));

    // emits 'finishing' to inform listener(s) we are sending 'EOF' to close stream
    if (this.listeners('finishing').length > 0) {
        this.emit('finishing', function(){  closeStream(); });
    } else {
        closeStream();
    }
};

function OSMArangoImpExport(options) {

    events.EventEmitter.call(this);
    
    var primaryElements = ['node', 'way', 'relation'];
    var subElementName = ['tag', 'nd', 'member'];

    this.mode = (!options.mode) ? 'arangoimp' : (['arangoimp', 'json'].indexOf(options.mode.toLowerCase()) > -1) ? options.mode.toLowerCase() : 'arangoimp';
    this.source = options.source;
    this.destination = options.destination;

    logger('Options:', this.mode, this.source, this.destination);

    // size of source in bytes
    this.sourceBytesTotal = 0;
    // size of bytes read from source so far
    this.sourceBytesRead = 0;
    // size of parsed data bytes queued for write
    this.destinationBytesQueued = 0;
    // size of parsed data bytes written so far
    this.destinationBytesWritten = 0;

    this._currentElement = null;
    this._stopped = false;
    this._started = false;
    this._enteredstart = false;
    this._emittedstart = false;

    // Stream pipelines:
    // ==================
    // [rstream = fs.createReadStream(source file)]  == pipe ==>  [parser]
    //
    // parser.onEndElement(data, fn(data){ datastream.push(data) });
    //
    // [datastream]  == pipe ==>  [wstream = fs.createWriteStream(destination file)]
    this._rstream = null;
    this._wstream = null;
    this._datastream = null;

    // initialize datastream and its event handlers
    this._datastream = new DataStream(function(){
    });
    this._datastream.on('data', (function (data) {
        logger('_datastream data');
        this.destinationBytesQueued += data.length;
        this.destinationBytesWritten = this._wstream.bytesWritten;
    }).bind(this));
    this._datastream.on('finishing', function (callback) {
        logger('_datastream finishing');
        if (this.mode === 'json') {
            logger('closing JSON array' );
            this._datastream.queue('\n]');
        }
        callback();
    }.bind(this));
    this._datastream.on('end', (function () {
        logger('_datastream end, bytes received', this.destinationBytesQueued, 'bytes');
    }).bind(this));

    // initialize parser and its event handlers
    this._parser = new expat.Parser();
    this._parser.on('pipe', (function (src) {
        logger('_parser received pipe from ',src.path);
    }).bind(this));
    this._parser.on('startElement', (function (name, attrs) {
        var funcname = ['__parse_', name].join('');
        if ((primaryElements.indexOf(name) > -1 && (OSMArangoImpExport.prototype[funcname])) ||
            (subElementName.indexOf(name) > -1 && this._currentElement !== null && (OSMArangoImpExport.prototype[funcname]))) {
            OSMArangoImpExport.prototype[funcname].call(this, attrs);
        }
    }).bind(this));

    this._parser.on('endElement', (function (name) {
        if (this._stopped) return;

        if ((primaryElements.indexOf(name) > -1)) {
            var clone = extend({}, this._currentElement);
            this._currentElement = null;

            if (this.listeners(name).length > 0) {
                this.emit(name, clone, (function (data) {
                    if (data){ this.__addToDataStream(data); }
                }.bind(this)));
            } else {
                this.__addToDataStream(clone);
            }
        }
    }).bind(this));

    this._parser.on('error', (function (err) {
        this.emit('error', err);
    }).bind(this));

    this._parser.on('end', (function () {
        // note: maybe triggered twice, due to race conditions
        // between someone calling this.stop() manually (which calls parser.end())
        // and when parser has truly finish reading the file.
        logger('_parser end');
    }).bind(this));

}

util.inherits(OSMArangoImpExport, events.EventEmitter);

OSMArangoImpExport.prototype.start = function () {
    logger('OSMArangoImpExport.start()');

    if (this._enteredstart) return;
    this._enteredstart = true;

    var stat = fs.statSync(this.source);
    this.sourceBytesTotal = stat.size;

    this._rstream = fs.createReadStream(this.source);
    this._rstream.on('open', (function () {
        logger('_rstream open, bytes total: ', this.sourceBytesTotal, 'bytes');
        this._rstream.pipe(this._parser);
    }).bind(this));
    this._rstream.on('close', (function () {
        logger('_rstream closed, bytes read: ', this.sourceBytesRead, ' of ', this.sourceBytesTotal, 'bytes');
        this._parser.end();
        this._datastream.end();
    }).bind(this));
    this._rstream.on('data', (function (data) {
        if (this._emittedstart && !this._stopped) this.emit('data', data);
        if (!this._stopped) this.sourceBytesRead += data.length;
        logger('_rstream data, bytes read: ', this.sourceBytesRead, ' of ', this.sourceBytesTotal, 'bytes');
    }).bind(this));

    this._wstream = fs.createWriteStream(this.destination);
    this._wstream.on('open', (function () {
        logger('_wstream open, destination: ', this.destination);
        this._datastream.pipe(this._wstream);
        this.emit('start', this.sourceBytesTotal);
        this._emittedstart = true;
    }).bind(this));
    this._wstream.on('pipe', (function (src) {
        logger('_wstream received pipe from ', (src === this._datastream)?'_datastream':'{unknown pipe}');
    }).bind(this));
    this._wstream.on('close', (function () {
        this.destinationBytesWritten = this._wstream.bytesWritten;
        logger('_wstream closed, bytes written: ', this._wstream.bytesWritten, 'bytes');
        logger('_wstream emitting end event');
        this.emit('end', this.destination, this.destinationBytesWritten);
    }).bind(this));
};

OSMArangoImpExport.prototype.stop = function () {
    logger('OSMArangoImpExport.stop()');

    if (this._stopped) return;
    this._stopped = true;

    // starts a chain reaction of stream closures
    // rstream.close() -> parser.end(), datastream.end() -> wstream.close() (automatically)
    this._rstream.close();

};

OSMArangoImpExport.prototype.__addToDataStream = function (data) {
    if (this._stopped) return;
    if (!this._datastreamAddedFirstLine) {
        // first line for mode=json has to be the square brackets for arrays
        // closing bracket is handled in on('finishing') event handler
        if (this.mode === 'json') { this._datastream.queue('[' + EOL); }
        this._datastreamAddedFirstLine = true;
    } else {
        this._datastream.queue((this.mode === 'json') ? ',' + EOL : EOL);
    }
    this._datastream.queue(JSON.stringify(data));
};

OSMArangoImpExport.prototype.__parseCommonAttributes = function (attrs) {
    // Reference: http://wiki.openstreetmap.org/wiki/Data_Primitives#Common_attributes
    var obj = {};
    obj._key = parseInt(attrs.id);  // map it to _key so we can import edges
    obj.id = parseInt(attrs.id);
    obj.user = attrs.user;
    obj.uid = parseInt(attrs.uid);
    obj.timestamp = new Date(attrs.timestamp);
    obj.visible = (attrs.visible) ? (['', attrs.visible].join('').toLowerCase() !== 'false') : true; // default: true // TODO: unittest
    obj.version = parseInt(attrs.version);
    obj.changeset = parseInt(attrs.changeset);
    obj.tags = {};
    return obj;
};

OSMArangoImpExport.prototype.__parse_node = function (attrs) {
    var obj = this.__parseCommonAttributes(attrs);
    obj.type = 'node';
    obj.lat = parseFloat(attrs.lat);
    obj.lon = parseFloat(attrs.lon);
    this._currentElement = obj;
};

OSMArangoImpExport.prototype.__parse_way = function (attrs) {
    var obj = this.__parseCommonAttributes(attrs);
    obj.type = 'way';
    obj.nodes = [];
    this._currentElement = obj;
};

OSMArangoImpExport.prototype.__parse_relation = function (attrs) {
    var obj = this.__parseCommonAttributes(attrs);
    obj.type = 'relation';
    obj.members = [];
    this._currentElement = obj;
};

OSMArangoImpExport.prototype.__parse_tag = function (attrs) {
    if (!this._currentElement) return;
    if (!this._currentElement.tags) this._currentElement.tags = {};
    this._currentElement.tags[attrs.k] = attrs.v;
};

OSMArangoImpExport.prototype.__parse_nd = function (attrs) {
    if (!this._currentElement) return;
    if (!this._currentElement.nodes) this._currentElement.nodes = [];
    this._currentElement.nodes.push(parseInt(attrs.ref));
};

OSMArangoImpExport.prototype.__parse_member = function (attrs) {
    var member = {
        type: attrs.type,
        role: attrs.role || null,
        ref: parseInt(attrs.ref)
    };

    if (!this._currentElement) return;
    if (!this._currentElement.members) this._currentElement.members = [];
    this._currentElement.members.push(member);

};

module.exports = OSMArangoImpExport;