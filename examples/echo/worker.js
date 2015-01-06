var fs = require('fs');
var Worker = require('./../../index').Worker;

var conf = JSON.parse(fs.readFileSync(__dirname + '/../config.json', 'UTF-8'));

var worker = new Worker('tcp://' + conf.broker.host + ':' + conf.broker.port, 'echo')
worker.start();

worker.on('error', function(e) {
  console.log('ERROR', e);
});

worker.on('request', function(inp, rep) {
  console.log("NEW REQUEST", inp)
  for (var i = 0; i < 10; i++) {
    rep.write('reply-partial');
  }
  rep.end('reply-final');
});
