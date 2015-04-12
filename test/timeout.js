var PIGATO = require('../');
var chai = require('chai');
var uuid = require('node-uuid');

var location = 'inproc://#';

describe('TIMEOUT', function () {
  var bhost = location + uuid.v4();

  var broker = new PIGATO.Broker(bhost);
  broker.start(function() {});

  after(function(done) {
    broker.stop(done);
  });

  it('Resend Request on Worker death', function (done) {
    var ns = uuid.v4();
    this.timeout(15000);

    var client = new PIGATO.Client(bhost);
    var worker = new PIGATO.Worker(bhost, ns);
    var backupWorker = new PIGATO.Worker(bhost, ns);
    var chunk = 'foo';

    client.start();
    worker.start();

    worker.on('request', function(inp, res) {
      res.end(inp + ':bar');
    });

    backupWorker.on('request', function(inp, res) {
      res.end(inp + ':bar');
    });

    var counter = 0;
    function request() {
      client.request(ns, chunk, { timeout: 5000 })
      .on('data', function (data) {
        chai.assert.equal(data, chunk + ':bar');
      })
      .on('end', function () {
        if(counter >= 3) {
          stop(null);
        } else {
          setTimeout(function () {
            worker.stop(null);
            request();
          }, 2000)
        }
        counter++;
      })
      .on('error', function (err) {
        if (err === 'C_TIMEOUT' && counter < 3) {
          request();
        } else {
          stop('All workers died');
        }
      });
    }

    request();
    setTimeout(function () {
      backupWorker.start();
    }, 10000);

    function stop(err) {
      client.stop();
      worker.stop();
      done(err);
    }
  })
});
