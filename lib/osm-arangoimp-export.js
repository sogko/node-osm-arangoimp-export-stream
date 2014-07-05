var expat = require('node-expat');
var fs = require('graceful-fs');
var util = require('util');
var extend = require('util')._extend;
var events = require('events');
var EOL = require('os').EOL;
var logger = require('./logger');

function OSMArangoImpExport(options) {

    events.EventEmitter.call(this);
    
    var primaryElements = ['node', 'way', 'relation'];
    var subElementName = ['tag', 'nd', 'member'];

    this.mode = (!options.mode) ? 'arangoimp' : (['arangoimp', 'json'].indexOf(options.mode.toLowerCase()) > -1) ? options.mode.toLowerCase() : 'arangoimp';
    this.source = options.source;
    this.destination = options.destination;

    logger(this.mode, this.source, this.destination, 123, 12.12, {'oject':12}, [12,'adasd',{as:23}]);

    this._currentElement = null;
    this._stopped = false;

    this._rstream = null;
    this._wstream = null;
    this._wstreamReady = false;
    this._wstreamHasNotWritten = true;
    this._wstreamPendingWrites = [];

    this._parser = expat.createParser();
    this._parser.on('startElement', (function (name, attrs) {
        var funcname = ['__parse_', name].join('');
        if ((primaryElements.indexOf(name) > -1 && (OSMArangoImpExport.prototype[funcname])) ||
            (subElementName.indexOf(name) > -1 && this._currentElement !== null && (OSMArangoImpExport.prototype[funcname]))) {
            OSMArangoImpExport.prototype[funcname].call(this, attrs);
        }
    }).bind(this));

    this._parser.on('endElement', (function (name) {
        if ((primaryElements.indexOf(name) > -1)) {
            var post = function (name, data) {
                this.__writeToStream(data);
            }.bind(this);
            var clone = extend({}, this._currentElement);
            if (this.listeners(name).length > 0) {
                this.emit(name, clone, function (data) {
                    post(name, data);
                });
            } else {
                post(name, clone);
            }
            this._currentElement = null;

        }
    }).bind(this));

    this._parser.on('error', (function (err) {
        this.emit('error', err);
    }).bind(this));

    this._parser.on('end', (function () {
        logger('called parsed end');
        var file_paths = [];
        this.stop();
    }).bind(this));

}

util.inherits(OSMArangoImpExport, events.EventEmitter);

OSMArangoImpExport.prototype.start = function () {
    this._wstream = fs.createWriteStream(this.destination);
    this._wstream.on('open', (function () {
        this._wstreamReady = true;
    }).bind(this));

    this._rstream = fs.createReadStream(this.source);
    this._rstream.on('open', (function () {
        this._rstream.pipe(this._parser);
    }).bind(this));
};

OSMArangoImpExport.prototype.stop = function () {
    if (this._stopped) return;
    this._stopped = true;

    this.__endWriteStream();
    this._parser.stop();
    this.emit('end', this.destination);
};

OSMArangoImpExport.prototype.__endWriteStream = function () {
    if (this.mode === 'json') this._wstream.write('\n]');
    this._wstreamReady = false;
    this._wstream.end();    // emit end on _wstream.end
};

OSMArangoImpExport.prototype.__writeToStream = function (data) {
    var _writeNewLine = function () {
        if (this._wstreamHasNotWritten) {
            // first line for mode=json has to be the square brackets for arrays
            if (this.mode === 'json') this._wstream.write('[' + EOL);
            this._wstreamHasNotWritten = false;
        } else {
            this._wstream.write((this.mode === 'json') ? ',' + EOL : EOL);
        }
    }.bind(this);

    var _write = function (d) {
        _writeNewLine();
        this._wstream.write(JSON.stringify(d));
    }.bind(this);

    if (this._wstreamReady) {
        while (this._wstreamPendingWrites.length > 0) {
            _write(this._wstreamPendingWrites.pop());
        }
        _write(data);
    } else {
        this._wstreamPendingWrites.push(data);
    }


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
    obj.nodes = [];
    this._currentElement = obj;
};

OSMArangoImpExport.prototype.__parse_relation = function (attrs) {
    var obj = this.__parseCommonAttributes(attrs);
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