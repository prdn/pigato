var PIGATO = require('../');
var chai = require('chai');
var uuid = require('shortid');

var location = 'inproc://#1';

describe('BASE', function() {
  var broker = new PIGATO.Broker(location)
  broker.start(function() {});

  it('Client partial/final request (stream)', function(done) {
    var ns = uuid.generate();
    var chunk = 'foo';

    var worker = new PIGATO.Worker(location, ns);

    worker.on('request', function(inp, res) {
      for (var i = 0; i < 5; i++) {
        res.write(inp + i);
      }
      res.end(inp + (i));
    });

    worker.start();

    var client = new PIGATO.Client(location);
    client.start();

    var repIx = 0;

    client.request(
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

  it('Client partial/final request (callback)', function(done) {
    var ns = uuid.generate();
    var chunk = 'foo';

    var worker = new PIGATO.Worker(location, ns);

    worker.on('request', function(inp, res) {
      for (var i = 0; i < 5; i++) {
        res.write(inp + i);
      }
      res.end(inp + 'FINAL' + (++i));
    });

    worker.start();

    var client = new PIGATO.Client(location);
    client.start();

    var repIx = 0;

    client.request(
      ns, chunk,
      function(err, data) {
        chai.assert.equal(data, chunk + (repIx++));
      },
      function(err, data) {
        chai.assert.equal(data, chunk + 'FINAL' + (++repIx));
        stop();
      }
    );

    function stop() {
      worker.stop();
      client.stop();
      done();
    }
  });

  it('JSON Client partial/final request (callback)', function(done) {
    var ns = uuid.generate();
    var chunk = { foo: 'bar' };

    var worker = new PIGATO.Worker(location, ns);

    worker.on('request', function(inp, res) {
      res.end(chunk);
    });

    worker.start();

    var client = new PIGATO.Client(location);
    client.start();

    var repIx = 0;

    client.request(
      ns, 'foo',
      undefined,
      function(err, data) {
        chai.assert.deepEqual(data, chunk);
        stop();
      }
    );

    function stop() {
      worker.stop();
      client.stop();
      done();
    }
  });

  it('Worker reject', function(done) {
    var ns = uuid.generate();
    this.timeout(10 * 1000);

    var chunk = 'NOT_MY_JOB';
    var chunk_2 = 'DID_MY_JOB';

    var workers = [];

    function spawn(fn) {
      var worker = new PIGATO.Worker(location, ns);
      worker.on('request', fn);

      worker.start();
      workers.push(worker);
    };

    spawn(function(inp, res) {
      res.reject(chunk);
      spawn(function(inp, res) {
        res.end(chunk_2);
      });
    });

    var client = new PIGATO.Client(location);
    client.start();

    client.request(
      ns, chunk
    ).on('data', function(data) {
        chai.assert.equal(data, chunk_2);
      }).on('end', function() {
        stop();
      });

    function stop() {
      workers.forEach(function(worker) {
        worker.stop();
      });
      client.stop();
      done();
    }
  });

  it('Client error request (stream)', function(done) {
    var ns = uuid.generate();
    var chunk = 'SOMETHING_FAILED';

    var worker = new PIGATO.Worker(location, ns);

    worker.on('request', function(inp, res) {
      res.error(chunk);
    });

    worker.start();

    var client = new PIGATO.Client(location);
    client.start();

    client.request(
      ns, chunk
    ).on('error', function(err) {
        chai.assert.equal(err, chunk);
        stop();
      });

    function stop() {
      worker.stop();
      client.stop();
      done();
    }
  });

  it('Client error request (callback)', function(done) {
    var ns = uuid.generate();
    var chunk = 'SOMETHING_FAILED';

    var worker = new PIGATO.Worker(location, ns);

    worker.on('request', function(inp, res) {
      res.error(chunk);
    });

    worker.start();

    var client = new PIGATO.Client(location);
    client.start();

    client.request(
      ns, chunk,
      undefined,
      function(err, data) {
        chai.assert.equal(err, chunk);
        chai.assert.equal(data, null);
        stop();
      }
    );

    function stop() {
      worker.stop();
      client.stop();
      done();
    }
  });

  it('Client error timeout (stream)', function(done) {
    var ns = uuid.generate();
    this.timeout(5000);

    var client = new PIGATO.Client(location);
    client.start();

    client.request(
      ns, 'foo',
      { timeout: 1000 }
    ).on('error', function(err) {
        chai.assert.equal(err, 'C_TIMEOUT');
        stop();
      });

    function stop() {
      client.stop();
      done();
    }
  });

  it('Client error timeout (callback)', function(done) {
    var ns = uuid.generate();
    this.timeout(5000);

    var client = new PIGATO.Client(location);
    client.start();

    client.request(
      ns, 'foo',
      undefined,
      function(err, data) {
        chai.assert.equal(err, 'C_TIMEOUT');
        stop();
      },
      { timeout: 1000 }
    );

    function stop() {
      client.stop();
      done();
    }
  });
});

describe('Resend after timout', function () {
  var broker = new PIGATO.Broker(location);
  broker.start(function() {});

  it('Supports resending after worker dies', function (done) {
    var ns = uuid.generate();
    this.timeout(15000);
    var client = new PIGATO.Client(location);
    var worker = new PIGATO.Worker(location, ns);
    var backupWorker = new PIGATO.Worker(location, ns);
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
      client.request(ns, chunk, {timeout: 5000})
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
