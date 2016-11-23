var PIGATO = require('../');
var chai = require('chai');
var uuid = require('uuid');

var bhost = 'inproc://#' + uuid.v4();
//var bhost = 'tcp://0.0.0.0:2020';

var broker = new PIGATO.Broker(bhost);

describe('CONCURRENCY', function() {
  
  before(function(done) {
    broker.conf.onStart = done;
    broker.start();
  });
  
  after(function(done) {
    broker.conf.onStop = done;
    broker.stop();
  });

  it('Base', function(done) {
    var ns = uuid.v4();
    var chunk = 'bar';

    var cc = 20;

    var worker = new PIGATO.Worker(bhost, ns, { concurrency: cc });

    var reqIx = 0;

    worker.on('request', function(inp, res) {
      ++reqIx;

      var it = setInterval(function() {
        if (reqIx < cc) {
          return;
        }

        clearInterval(it);

        res.end(chunk);
      }, 10);
    });

    worker.start();

    var client = new PIGATO.Client(bhost);
    client.start();

    var repIx = 0;

    for (var i = 0; i < cc; i++) {
      client.request(
        ns, chunk,
        undefined,
        function(err, data) {
          chai.assert.deepEqual(data, chunk);
          ++repIx;
          if (repIx === cc) {
            stop();
          }
        }
      );
    }

    function stop() {
      worker.stop();
      client.stop();
      done();
    }
  });
});
