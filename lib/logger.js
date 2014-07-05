/**
 * NodeJS logger with datetime stamp just because.
 *
 * Reference:
 * NodeJS implementation of console.log: https://github.com/joyent/node/blob/master/lib/console.js#L52-L54
 * For padded datetime stamp: http://stackoverflow.com/a/12550320/245196
 *
 * Hold onto your socks now.
 */
var util = require('util');
var EOL = require('os').EOL;

function pad(n){ return n<10 ? ['0',n].join('') : n; }
function log(){
    if (arguments instanceof Object){ arguments = Array.prototype.slice.call(arguments, 0);}
    var d = new Date();
    d = ['[',d.getUTCFullYear(),'-',pad(d.getUTCMonth()+1),'-',pad(d.getUTCDate()),'T',pad(d.getUTCHours()),':',pad(d.getUTCMinutes()),':',pad(d.getUTCSeconds()),'Z] ' ].join('');
    process.stdout.write(d);
    arguments.forEach(function(o){process.stdout.write(util.format(o)+' ');});
    process.stdout.write(EOL);

}
module.exports = log;