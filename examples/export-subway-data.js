var OSMArangoImpExport = require('./../');

var source = __dirname + '/data/20147994052921-route=subway.osm';

var options = {

  // destination output folder
  destination: __dirname + '/output2/',

  // custom output collections, in addition to the
  // default 'nodes', 'ways' and 'relations' collections
  collections: ['stops', 'services'],

  // output files will be vanilla valid JSON
  mode: 'json',

  // remove cache db after completion, default to false
  keepCache: false

};

var exporter = new OSMArangoImpExport(options);

exporter.on('node', function (node, callback) {

  // Extract station stops
  if (node.tags.railway === 'station') {

    var stop = {};
    stop.id = node.id;
    stop.lat = node.lat;
    stop.lon = node.lon;
    stop.name = node.tags.name;
    stop.description = [stop.name, 'Station'].join(' ');

    callback('stops', stop, 'nodes', node);
    // equivalent callback('stops', [stop], 'nodes', node);

  } else {
    callback(node);
    // equivalent to callback('nodes', node);
  }

});

exporter.on('way', function (way, nodes, callback) {

  // if we choose to not process an entity, simply pass back the object
  callback(way);

});

exporter.on('relation', function (relation, members, callback) {

  var service = {};
  service.id = relation.id;
  service.name = relation.tags.ref;
  service.description = relation.tags.ref;
  service.operator = relation.tags.operator;
  service.stops = [];

  for (var i in relation.members) {
    var m = relation.members[i];
    if (m.type === 'node' && m.role === 'stop') {
      // full node/ways object can be retrieved from members args
      var node = members.nodes[m.ref];
      service.stops.push(node.tags.name);
    }
  }

  callback(
    'services', service,
    'relations', relation);
});

exporter.on('error', function (err) {
  console.log('error', err);
});

// kick start exporter
exporter.open(source);

exporter.on('end', function () {
  console.log('Done! Output files are available at: ', exporter.destination);
});