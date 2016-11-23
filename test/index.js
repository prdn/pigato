var PIGATO = require('../');
var chai = require('chai');
var uuid = require('uuid');

var bhost = 'inproc://#' + uuid.v4();
//var bhost = 'tcp://0.0.0.0:2020';

var broker = new PIGATO.Broker(bhost);

describe('BASE', function() {

  before(function(done) {
    broker.conf.onStart = done;
    broker.start();
  });
  
  after(function(done) {
    broker.conf.onStop = done;
    broker.stop();
  });

  it('Client partial/final Request (stream)', function(done) {
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

  it('Client partial/final Request (callback)', function(done) {
    var ns = uuid.v4();
    var chunk = 'foo';

    var worker = new PIGATO.Worker(bhost, ns);

    worker.on('request', function(inp, res) {
      for (var i = 0; i < 5; i++) {
        res.write(inp + i);
      }
      res.end(inp + 'FINAL' + (++i));
    });

    worker.start();

    var client = new PIGATO.Client(bhost);
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

  it('JSON Client partial/final Request (callback)', function(done) {
    var ns = uuid.v4();
    var chunk = {
      foo: 'bar'
    };

    var worker = new PIGATO.Worker(bhost, ns);

    worker.on('request', function(inp, res) {
      res.end(chunk);
    });

    worker.start();

    var client = new PIGATO.Client(bhost);
    client.start();

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
    var ns = uuid.v4();

    var chunk = 'NOT_MY_JOB';
    var chunk_2 = 'DID_MY_JOB';

    var workers = [];

    function spawn(fn) {
      var worker = new PIGATO.Worker(bhost, ns);
      worker.on('request', fn);

      worker.start();
      workers.push(worker);
    }

    spawn(function(inp, res) {
      res.reject(chunk);
      spawn(function(inp, res) {
        res.end(chunk_2);
      });
    });

    var client = new PIGATO.Client(bhost);
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

  it('Broker Request retry', function(done) {
    var ns = uuid.v4();

    var chunk = 'foo';

    var workers = [];

    function spawn(id) {
      var worker = new PIGATO.Worker(bhost, ns);

      worker.on('request', function(inp, res) {
        if (id == 'w1') {
          firstRequestDone();
        } else {
          res.end(chunk + '/' + id);
        }
      });

      worker.start();
      workers.push(worker);
      return worker;
    }

    var client = new PIGATO.Client(bhost);
    client.start();

    client.request(
      ns, chunk, {
        retry: 1
      }
    ).on('data', function(data) {
      chai.assert.equal(data, chunk + '/w2');
    }).on('end', function() {
      stop();
    });

    spawn('w1');

    function firstRequestDone() {
      setTimeout(function() {
        spawn('w2');
        workers[0].stop();
      }, 10);
    }

    function stop() {
      workers.forEach(function(worker) {
        worker.stop();
      });
      client.stop();
      done();
    }
  });

  it('Client error request (stream)', function(done) {
    var ns = uuid.v4();
    var chunk = 'SOMETHING_FAILED';

    var worker = new PIGATO.Worker(bhost, ns);

    worker.on('request', function(inp, res) {
      res.error(chunk);
    });

    worker.start();

    var client = new PIGATO.Client(bhost);
    client.start();

    client.request(ns, chunk)
      .on('error', function(err) {
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
    var ns = uuid.v4();
    var chunk = 'SOMETHING_FAILED';

    var worker = new PIGATO.Worker(bhost, ns);

    worker.on('request', function(inp, res) {
      res.error(chunk);
    });

    worker.start();

    var client = new PIGATO.Client(bhost);
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
    var ns = uuid.v4();

    var client = new PIGATO.Client(bhost, {
      heartbeat: 25
    });
    client.start();

    client.request(ns, 'foo', {
      timeout: 60
    })
      .on('error', function(err) {
        chai.assert.equal(err, 'C_TIMEOUT');
        stop();
      });

    function stop() {
      client.stop();
      done();
    }
  });

  it('Client error timeout (callback)', function(done) {
    var ns = uuid.v4();

    var client = new PIGATO.Client(bhost, {
      heartbeat: 25
    });
    client.start();

    client.request(
      ns, 'foo',
      undefined,
      function(err, data) {
        chai.assert.equal(err, 'C_TIMEOUT');
        chai.assert.equal(data, undefined);
        stop();
      }, {
        timeout: 60
      }
    );

    function stop() {
      client.stop();
      done();
    }
  });
});
