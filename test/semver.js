var PIGATO = require('../');
var chai = require('chai');
var uuid = require('uuid');

var bhost = 'inproc://#' + uuid.v4();
//var bhost = 'tcp://0.0.0.0:2020';

var broker = new PIGATO.Broker(bhost);


describe('SEMVER', function() {
  before(function(done) {
    broker.conf.onStart = done;
    broker.start();
  });

  after(function(done) {
    broker.conf.onStop = done;
    broker.stop();
  });

  it('allows services with semver in name', function(done) {
    var ns = uuid.v4() + '@1.2.3';
    this.timeout(5000);

    var client = new PIGATO.Client(bhost);

    var worker = new PIGATO.Worker(bhost, ns);
    worker.on('request', function(inp, rep) {
      rep.end(worker.conf.name);
    });
    worker.start();

    client.start();

    var workerId = worker.conf.name;
    client.request(ns, 'foo')
    .on('data', function(data) {
      chai.assert.equal(data, workerId);
    })
      .on('error', function(err) {
        stop(err);
      })
      .on('end', function() {
        stop();
      });

    function stop(err) {
      worker.stop();
      client.stop();
      done(err);
    }
  });

  it('can resolve services using semver', function(done) {
    var namePrefix = uuid.v4();
    var ns = namePrefix + '@1.2.3';
    this.timeout(5000);

    var client = new PIGATO.Client(bhost);

    var worker = new PIGATO.Worker(bhost, ns);
    worker.on('request', function(inp, rep) {
      rep.end(worker.conf.name);
    });
    worker.start();

    client.start();

    var workerId = worker.conf.name;
    client.request(namePrefix + '@^1.2.x', 'foo')
    .on('data', function(data) {
      chai.assert.equal(data, workerId);
    })
    .on('error', function(err) {
      stop(err);
    })
    .on('end', function() {
      stop();
    });

    function stop(err) {
      worker.stop();
      client.stop();
      done(err);
    }
  });
});
