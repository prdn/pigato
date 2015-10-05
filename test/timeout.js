var PIGATO = require('../');
var chai = require('chai'),
  assert = chai.assert;
var uuid = require('node-uuid');

var location = 'inproc://#';

describe('TIMEOUT', function() {
  var bhost = location + uuid.v4();
  var ns = uuid.v4();
  var broker = new PIGATO.Broker(bhost);

  var client = new PIGATO.Client(bhost, {
    heartbeat: 25
  });

  var chunk = 'foo';
  var worker = new PIGATO.Worker(bhost, ns);
  var backupWorker = new PIGATO.Worker(bhost, ns);

  before(function(done) {
    client.start();
    broker.conf.onStart = done;
    broker.start();
  });

  after(function(done) {
    broker.conf.onStop = done;
    broker.stop();
  });

  describe('When a server is launched', function() {

    before(function(done) {
      worker.start();
      worker.on('request', function(inp, res) {
        res.end(inp + ':bar');
      });
      done();
    });

    it('can be reached', function(done) {
      client.request(ns, chunk, {
        timeout: 60
      })
      .on('data', function(data) {
        chai.assert.equal(data, chunk + ':bar');
      })
      .on('end', done);
    });
  });


  describe('When a server is stop', function() {

    before(function(done) {
      worker.stop();
      setImmediate(done);
    });

    it('client get a timeout', function(done) {
      client.request(ns, chunk, {
        timeout: 50
      })
      .on('error', function(err) {
        chai.assert.notEqual(null, err);
        chai.assert.equal('C_TIMEOUT', err);
        done();
      });
    });
  });


  describe('When we launch a server during a request', function() {

    it('can reach the server and answer', function(done) {

      var timeoutBeforeLaunch = 50;
      var startTime = +new Date();
      client.request(ns, chunk, {
        timeout: 100
      })
      .on('data', function(data) {
        chai.assert.equal(data, chunk + ':bar');
      })
      .on('error', function(err) {
        assert.ok(!err, 'Should not be here');
      })
      .on('end', function(){

        var endTime = +new Date();
        assert.ok( endTime - startTime > timeoutBeforeLaunch , 'response is to fast');
        done();
      });

      setTimeout(function() {
        backupWorker.start();
        backupWorker.on('request', function(inp, res) {
          res.end(inp + ':bar');
        });
      }, timeoutBeforeLaunch);
    });
  });


  describe('After that I can do something fast', function() {

    it('can reach the server and answer', function(done) {
      client.request(ns, chunk, {
        timeout: 60
      })
      .on('data', function(data) {
        chai.assert.equal(data, chunk + ':bar');
      })
      .on('error', function(err) {
        assert.ok(!err, 'Should not be here');
      })
      .on('end', done);
    });
  });
});
/*
  it('Can reach a launched server ', function(done) {


  });


  it('Timeout once their is no more server', function(done) {


  });

  describe('When I launch a server while a request is launch', function() {

    it('successfully wait between tiemout delay for a new server', function() {

    });

  });


  it('Resend Request on Worker death', function(done) {
    var chunk = 'foo';


    worker.start();

    worker.on('request', function(inp, res) {
      res.end(inp + ':bar');
    });

    backupWorker.on('request', function(inp, res) {
      res.end(inp + ':bar');
    });

    var counter = 0;

    var workerRuning = true;

    function request() {
      client.request(ns, chunk, {
          timeout: 60
        })
        .on('data', function(data) {
          chai.assert.equal(data, chunk + ':bar');
        })
        .on('end', function() {
          if (counter >= 3) {
            stop(null);
          } else {
            setTimeout(function() {
              if (workerRuning) {
                worker.stop(null);
                workerRuning = false;
              }
              request();
            }, 200);
          }
          counter++;
        })
        .on('error', function(err) {
          if (err === 'C_TIMEOUT' && counter < 3) {
            request();
          } else {
            stop('All workers died');
          }
        });
    }

    request();
    setTimeout(function() {
      backupWorker.start();
    }, 100);

    function stop(err) {
      client.stop();
      worker.stop();
      done(err);
    }
  })
}); */
