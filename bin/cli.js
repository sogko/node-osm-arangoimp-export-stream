var OSMArangoImpExportStream = require('./../');
var ProgressBar = require('progress');
var path = require('path');
var fs = require('fs');
var JSONStream = require('JSONStream');

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
  .describe('p', 'Show progress, useful for large files. Set to "no" to hide it')

  .default('c', 'yes')
  .alias('c', 'check')
  .describe('c', 'Check if output is a valid JSON file if mode=\'json\'')

  .boolean('h')
  .alias('h', 'help')
  .describe('h', 'Show help')
  .argv;

if (argv.help){
  require('yargs').showHelp();
  return;
}

var options = {
  destination: argv.destination,
  mode: argv.mode
};

var exportStream = new OSMArangoImpExportStream(options);
var sourceBytesTotal = fs.statSync(argv.source).size;
var bar;

exportStream.open(argv.source);

exportStream.on('start', function () {

  if (argv.progress) {
    bar = new ProgressBar('Streaming data from ' + path.basename(argv.source) + ' [:bar] :percent / Total :current of :total bytes / Elapsed: :elapseds  / ETA: :etas ', {
      complete: '=',
      incomplete: ' ',
      width: 20,
      total: sourceBytesTotal
    });
  }
});

exportStream.on('error', function (err) {
  console.log('error', err);
  throw new Error(err);
});

if (argv.progress) {
  exportStream.on('incoming_data', function (chunk) {
    bar.tick(chunk.length);
  });
}

exportStream.on('end', function () {
  console.log('\nFinished parsing our .osm file');
  console.log('Bytes read from incoming stream:', exportStream.bytesRead, 'bytes');
  console.log('Bytes written to outgoing stream:', exportStream.bytesWritten, 'bytes\n');

  if (argv.mode === 'json' && argv.check === 'yes') {
    console.log('Checking that written file is a valid JSON:', exportStream.destination);

    var jsonParser = JSONStream.parse(['rows', true]);
    fs.createReadStream(exportStream.destination).pipe(jsonParser);

    var isValidJSON = true;
    jsonParser.on('error', function(err){
      // if we receive error, json is invalid
      console.log('JSON error', err);
      isValidJSON = false;
    });
    jsonParser.on('close', function(){
      console.log('JSON file check:', (isValidJSON)?'OK' : 'ERROR');
      console.log();
    });
  }


});



