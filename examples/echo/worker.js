var fs = require('fs');
var Worker = require('./../../index').Worker;

var conf = JSON.parse(fs.readFileSync(__dirname + '/../config.json', 'UTF-8'));

var worker = new Worker(conf.broker.host, 'echo')
worker.start();

worker.on('error', function(e) {
  console.log('ERROR', e);
});

worker.on('request', function(inp, rep, opts) {
  rep.opts.cache = 10000;
  rep.end(inp);
});
