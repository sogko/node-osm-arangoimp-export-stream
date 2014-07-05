var expat = require('node-expat');
var StreamSearch = require('streamsearch');
//var fs = require('fs');
var fs = require('graceful-fs');
var util = require('util');
var extend = require('util')._extend;
var events = require('events');

process.on('uncaughtException', function(err) {
    console.log('uncaughtException', util.inspect(err));
    if (err.code === 'EBADF'){
        // let through
    } else {
        process.exit();
    }
});

function OSMArangoImpExport(options) {

    var _this = this;
    var elementNames = ['node', 'way', 'relation'];
    var subElementNames = ['tag', 'nd', 'member'];

    events.EventEmitter.call(this);

    this.source = options.source;
    this._parser = expat.createParser();
    this._currentElement = null;
    this._nodeReferences = {};
    this._parser.on('pause', function (){
        console.log('PARSER PAUSED');
    });
    this._parser.on('resume', function (){
        console.log('PARSER RESUME');
    });
    this._parser.on('startElement', function (name, attrs) {

        var funcname = ['_parse_', name].join('');
        if ((elementNames.indexOf(name) > -1 && (OSMArangoImpExport.prototype[funcname])) ||
            (subElementNames.indexOf(name) > -1 && _this._currentElement !== null && (OSMArangoImpExport.prototype[funcname]))) {
            OSMArangoImpExport.prototype[funcname].call(_this, attrs);
        }



    });

    this._parser.on('endElement', function (name) {
        if ( (elementNames.indexOf(name) > -1) || (subElementNames.indexOf(name) > -1) ){
            var clone = extend({}, _this._currentElement);
            if (elementNames.indexOf(name) > -1) { _this._currentElement = null; }

            var post = function(name, data){

                // check if id appears later
                if (name === 'node' && data && data.id){
                    var func = function(node_id, node){
                        var lastReadMatch = 0;
                        var fdReadStream;
                        var needle = new Buffer(node_id);
                        var searchStream = new StreamSearch(needle);

                        var closeFd = function(fd){
                            if (!fd) {
                                console.log('[Already CLOSED] fd', fd, node_id, lastReadMatch);
                                return;
                            }
                            try {
                                fs.closeSync(fdReadStream);
                                console.log('[CLOSED] fd', fd, node_id, lastReadMatch);
                                fd = null;
                            } catch(e){
                                console.log('[CLOSED ERROR] fd', fd, node_id, lastReadMatch);
                                fd = null;
                            }
                        };

                        try {
                            searchStream.on('info', function(isMatch, data, start, end) {
                                if (isMatch) {
                                    lastReadMatch++;
                                    if (lastReadMatch > 1){
                                        closeFd(fdReadStream);
                                        fdReadStream = null;
                                        _this._nodeReferences[node_id] = node;
                                    }
                                }
                            });

                            var readStream = fs.createReadStream(_this.source, {
                                autoClose: false // wont emit close event
                            });
                            readStream.on('open', function(fd) {
                                fdReadStream = fd;
                                console.log('readStream open', fdReadStream);
                            });
                            readStream.on('readable', function() {
                                var chunk = readStream.read();
                                searchStream.push(chunk);
                            });
                            readStream.on('end', function() {
                                console.log('readStream end', fdReadStream);
                                closeFd(fdReadStream);
                                fdReadStream = null;
                            });

                        } catch(e){
                            console.log('readStream ERROR caught', e);

                        }


                    };
                    func(''+data.id, extend({}, data));


                }

                // resolve references to make it available for listener
                if (name === 'nd'){
                    for (var i in _this._currentElement.__node_refs){
                        var ref_id = _this._currentElement.__node_refs[i];
                        console.log(ref_id, _this._nodeReferences[ref_id]);
                    }
                }

            };

            if (_this.listeners(name).length > 0){
                _this.emit(name, clone, function (data) { post(name, data); });
            } else {
                post(name, clone);
            }


        }
    });

    this._parser.on('error', function (err) {
        _this.emit('error', err);
    });

    this._parser.on('end', function () {

        if (name === 'nd'){
            for (var i in _this._currentElement.__node_refs){
                var ref_id = _this._currentElement.__node_refs[i];
                console.log(ref_id, _this._nodeReferences[ref_id]);
            }
        }

        var file_paths = [];
        _this.emit('end', file_paths);
    });

}

util.inherits(OSMArangoImpExport, events.EventEmitter);

OSMArangoImpExport.prototype.start = function () {
    this.reader = fs.createReadStream(this.source);
    this.reader.pipe(this._parser);
};

OSMArangoImpExport.prototype._parseCommonAttributes = function (attrs) {
    // Reference: http://wiki.openstreetmap.org/wiki/Data_Primitives#Common_attributes
    var obj = {};
    obj.id = parseInt(attrs.id);
    obj.user = attrs.user;
    obj.uid = parseInt(attrs.uid);
    obj.timestamp = new Date(attrs.timestamp);
    obj.visible = (attrs) ? (['', attrs].join('').toLowerCase() === 'true') : true;
    obj.version = parseInt(attrs.version);
    obj.changeset = parseInt(attrs.changeset);
    obj.tags = {};
    return obj;
};

OSMArangoImpExport.prototype._parse_node = function (attrs) {
    var obj = this._parseCommonAttributes(attrs);
    obj.lat = parseFloat(attrs.lat);
    obj.lon = parseFloat(attrs.lon);
    this._currentElement = obj;
};

OSMArangoImpExport.prototype._parse_way = function (attrs) {
    var obj = this._parseCommonAttributes(attrs);
    obj.nodes = [];
    this._currentElement = obj;
};

OSMArangoImpExport.prototype._parse_relation = function (attrs) {
    var obj = this._parseCommonAttributes(attrs);
    obj.members = [];
    this._currentElement = obj;
};

OSMArangoImpExport.prototype._parse_tag = function (attrs) {
    if (!this._currentElement) return;
    if (!this._currentElement.tags) this._currentElement.tags = {};
    this._currentElement.tags[attrs.k] = attrs.v;
};

OSMArangoImpExport.prototype._parse_nd = function (attrs) {
    if (!this._currentElement) return;
    if (!this._currentElement.__node_refs) this._currentElement.__node_refs = [];
    this._currentElement.__node_refs.push(parseInt(attrs.ref));
};

OSMArangoImpExport.prototype._parse_member = function (attrs) {
    var member = {
        type: attrs.type,
        role: attrs.role || null,
        __ref: parseInt(attrs.ref)
    };

    if (!this._currentElement) return;
    if (!this._currentElement.members) this._currentElement.members = [];
    this._currentElement.members.push(member);

};

OSMArangoImpExport.prototype._resolveReferences = function (type, ref) {


};

module.exports = OSMArangoImpExport;