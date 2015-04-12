var PIGATO = require('../');
var chai = require('chai');
var uuid = require('node-uuid');

var location = 'inproc://#';

describe('WILDCARDS', function () {
  var bhost = location + uuid.v4();

  var broker = new PIGATO.Broker(bhost);
  broker.start(function() {});

  after(function(done) {
    broker.stop(done);
  });

  it('Base', function (done) {
    var ns = uuid.v4();
    this.timeout(5000);

    var client = new PIGATO.Client(bhost);
    var worker = new PIGATO.Worker(bhost, ns + '*');
    var chunk = 'foo';

    worker.start();

    client.start();

    worker.on('request', function(inp, res) {
      res.end(inp + ':bar');
    });

    var rcnt = 0;

    function request() {
      client.request(ns + '-' + uuid.v4(), chunk, { timeout: 5000 })
      .on('data', function(data) {
        chai.assert.equal(data, chunk + ':bar');
      })
      .on('error', function (err) {
        stop(err);
      })
      .on('end', function() {
        rcnt++;
        if (rcnt === 5) {
          stop();
        }
      });
    }

    for (var i = 0; i < 5; i++) {
      request();
    }

    function stop(err) {
      client.stop();
      worker.stop();
      done(err);
    }
  })
});
