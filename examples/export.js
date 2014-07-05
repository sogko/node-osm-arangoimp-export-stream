var OSMArangoImpExport = require('./../lib/osm-arangoimp-export');
var fs = require('fs');
var path = require('path');
var ProgressBar = require('progress');

var options = {
    source: './data/sample.osm',
//    source: './data/201474148283180-route=bus.osm',
//    source: './data/singapore.osm',
    destination: './output.txt',
    mode: 'json'
};

var exporter = new OSMArangoImpExport(options);
var bar;

exporter.on('node', function (node, callback) {
    if (node.id === 1840701403 || node.id === 26782020) {
//        console.log('user stopped', node.id);
//        exporter.stop();
//        console.log('user skipped node ', node.id);
//        return callback(false);
    }
    callback(node);
});

exporter.on('way', function (way, callback) {
    var tags = way.tags;
    var nodes = way.nodes; // ordered array. unique member may appear multiple times

    if (nodes && nodes.indexOf(1840701403) > -1){
        nodes.splice(nodes.indexOf(1840701403), 1);
    }
    if (nodes && nodes.indexOf(26782020) > -1){
        nodes.splice(nodes.indexOf(26782020), 1);
    }
    callback(way);
});

exporter.on('relation', function (relation, callback) {
    var tags = relation.tags;
    var members = relation.members; // ordered array. unique member may appear multiple times

    var newMembers = [];
    for (var member in members) {
        var type = member.type;
        var ref = member.ref;
        if (ref === 1840701403 || ref === 26782020){
            // removed refs
        } else {
            newMembers.push(member);
        }
    }
    relation.members = newMembers;
    callback(relation);
});

exporter.on('error', function (err) {
    console.log('error', err);
});

exporter.on('end', function (file_paths, bytesWritten) {
//    bar.complete = true;
    console.log('end', file_paths, bytesWritten, 'bytes');
});

exporter.on('data', function (data) {
    bar.tick(data.length);
});

exporter.on('start', function (total_bytes) {
    console.log('start', total_bytes, 'bytes');

    bar = new ProgressBar('Reading from ' + path.basename(this.source) + ' [:bar] :percent / Total :current of :total bytes / Elapsed: :elapseds  / ETA: :etas ', {
        complete: '=',
        incomplete: ' ',
        width: 20,
        total: exporter.sourceBytesTotal
    });

});

exporter.start();




