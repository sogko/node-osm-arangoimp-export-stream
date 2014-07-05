var OSMArangoImpExport = require('./../');
var ProgressBar = require('progress');
var path = require('path');

var argv = require('yargs')
    .usage('\nUsage: $0\nExport .osm files to formats accepted by arangoimp for bulk import in to ArangoDB.')
    .example('$0 -s <source_path> -d <destination_path>', 'Typical usage')
    .example('$0 -s <source_path> -d <destination_path> -m arangoimp', 'Export as JSON-encoded data for batched bulk import (default)')
    .example('$0 -s <source_path> -d <destination_path> -mode json', 'Export as a valid JSON file,')
    .example('$0 -s <source_path> -d <destination_path> -p false', 'Hide progress summary')

    .demand('s')
    .alias('s', 'source')
    .describe('s', 'Location of source .osm file')

    .demand('d')
    .alias('d', 'dest')
    .alias('d', 'destination')
    .describe('d', 'Location of destination exported file')

    .default('m', 'arangoimp')
    .alias('m', 'mode')
    .describe('m', 'File export mode')

    .default('p', 'yes')
    .alias('p', 'progress')
    .describe('p', 'Show progress, useful for large files. Set to \"no\" to hide it')

    .boolean('h')
    .alias('h', 'help')
    .describe('h', 'Show help')
    .argv;

var options = {
    source: argv.source,
    destination: argv.destination,
    mode: argv.mode
};
var exporter = new OSMArangoImpExport(options);
var bar;

if (argv.help){
    require('yargs').showHelp();
    return;
}

exporter.on('error', function (err) {
    console.log('error', err);
});

exporter.on('end', function (destination, bytesWritten) {
    console.log('\nExported file to', destination, '('+bytesWritten, 'bytes)');
});

if (argv.progress) {
    exporter.on('data', function (data) {
        bar.tick(data.length);
    });
}
exporter.on('start', function () {
    if (argv.progress) {
        bar = new ProgressBar('Reading from ' + path.basename(this.source) + ' [:bar] :percent / Total :current of :total bytes / Elapsed: :elapseds  / ETA: :etas ', {
            complete: '=',
            incomplete: ' ',
            width: 20,
            total: exporter.sourceBytesTotal
        });
    }
});
exporter.start();




