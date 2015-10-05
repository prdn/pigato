var PIGATO = require('../');
var chai = require('chai');
var uuid = require('node-uuid');

var bhost = 'inproc://#' + uuid.v4();
//var bhost = 'tcp://0.0.0.0:2020';

var broker = new PIGATO.Broker(bhost);
   

describe('TARGET', function() {
  before(function(done) {
    broker.conf.onStart = done;
    broker.start();
  });

  after(function(done) {
    broker.conf.onStop = done;
    broker.stop();
  });

  it('Request for specific Worker', function(done) {
    var ns = uuid.v4();
    this.timeout(5000);

    var client = new PIGATO.Client(bhost);

    var workers = [];

    function spawn() {
      var worker = new PIGATO.Worker(bhost, ns);
      worker.on('request', function(inp, rep) {
        rep.end(worker.conf.name);
      });
      worker.start();
      workers.push(worker);
    }

    client.start();

    var samples = 10;

    for (var wi = 0; wi < samples; wi++) {
      spawn();
    }

    var rcnt = 0;

    function request() {
      var workerId = workers[Math.round(Math.random() * 1000 % 9)].conf.name;
      client.request(ns, 'foo', {
        workerId: workerId
      })
        .on('data', function(data) {
          chai.assert.equal(data, workerId);
        })
        .on('error', function(err) {
          stop(err);
        })
        .on('end', function() {
          rcnt++;
          if (rcnt === samples) {
            stop();
          }
        });
    }

    for (var i = 0; i < samples; i++) {
      request();
    }

    function stop(err) {
      workers.forEach(function(worker) {
        worker.stop();
      });
      client.stop();
      done(err);
    }
  });
});
