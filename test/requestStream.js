var PIGATO = require('../');
var chai = require('chai');
var uuid = require('node-uuid');

var location = 'inproc://#';

describe('BASE', function() {
  var bhost = location + uuid.v4();

  var broker = new PIGATO.Broker(bhost)
  broker.start(function() {});

  after(function(done) {
    broker.stop();
    setTimeout(function() {
      done();
    }, 1000);
  });

  it('Client requestStream', function(done) {
    var ns = uuid.v4();
    var chunk = 'foo';

    var worker = new PIGATO.Worker(bhost, ns);

    worker.on('request', function(inp, res) {
      for (var i = 0; i < 5; i++) {
        res.write(inp + i);
      }
      res.end(inp + (i));
    });

    worker.start();

    var client = new PIGATO.Client(bhost);
    client.start();

    var repIx = 0;

    client.requestStream(
      ns, chunk
    ).on('data', function(data) {
      chai.assert.equal(data, String(chunk + (repIx++)));
    }).on('end', function() {
      stop();
    });

    function stop() {
      worker.stop();
      client.stop();
      done();
    }
  });

});
