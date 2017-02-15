var Worker = require('./../../index').Worker;
var conf = require('../config.json');

var worker = new Worker(conf.broker.host, 'echo')
worker.start();

worker.on('error', function(e) {
  console.log('ERROR', e);
});

worker.on('request', function(inp, rep, opts) {
  rep.opts.cache = 10000;
  rep.end(inp);
});
