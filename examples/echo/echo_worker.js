var Worker = require('./../../index').Worker;

var worker = new Worker('tcp://localhost:55555', 'echo');
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
