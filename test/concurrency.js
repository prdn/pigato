var PIGATO = require('../');
var chai = require('chai');
var uuid = require('node-uuid');

var location = 'inproc://#';

describe('CONCURRENCY', function () {
  var bhost = location + uuid.v4();

  var broker = new PIGATO.Broker(bhost);
  broker.start(function() {});

  after(function(done) {
    broker.stop();
    done();
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
      }, 50);
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
