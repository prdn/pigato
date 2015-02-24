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

  it('Client partial/final request (stream)', function(done) {
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

  it('Client partial/final request (callback)', function(done) {
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

  it('JSON Client partial/final request (callback)', function(done) {
    var ns = uuid.v4();
    var chunk = { foo: 'bar' };

    var worker = new PIGATO.Worker(bhost, ns);

    worker.on('request', function(inp, res) {
      res.end(chunk);
    });

    worker.start();

    var client = new PIGATO.Client(bhost);
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
    var ns = uuid.v4();
    this.timeout(15 * 1000);

    var chunk = 'NOT_MY_JOB';
    var chunk_2 = 'DID_MY_JOB';

    var workers = [];

    function spawn(fn) {
      var worker = new PIGATO.Worker(bhost, ns);
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

  it('Client request retry', function(done) {
    var ns = uuid.v4();
    this.timeout(25 * 1000);

    var chunk = 'foo';

    var workers = [];

    function spawn(id) {
      var worker = new PIGATO.Worker(bhost, ns);
      worker.on('request', function(inp, res) {
        setTimeout(function() {
          res.end(chunk + '/' + id);
        }, 2000);
      });

      worker.start();
      workers.push(worker);

      return worker;
    };

    var client = new PIGATO.Client(bhost);
    client.start();

    client.request(
      ns, chunk, { retry: 1 }
    ).on('data', function(data) {
      chai.assert.equal(data, chunk + '/w2');
    }).on('end', function() {
      stop();
    });
    
    spawn('w1');

    setTimeout(function() {
      spawn('w2');
      workers[0].stop();
    }, 1000);

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
    this.timeout(5000);

    var client = new PIGATO.Client(bhost);
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
    var ns = uuid.v4();
    this.timeout(5000);

    var client = new PIGATO.Client(bhost);
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

describe('Wildcards', function () {
  var bhost = location + uuid.v4();

  var broker = new PIGATO.Broker(bhost);
  broker.start(function() {});

  after(function(done) {
    broker.stop();
    setTimeout(function() {
      done();
    }, 1000);
  });

  it('test1', function (done) {
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

describe('Resend after timout', function () {
  var bhost = location + uuid.v4();

  var broker = new PIGATO.Broker(bhost);
  broker.start(function() {});

  after(function(done) {
    broker.stop();
    setTimeout(function() {
      done();
    }, 1000);
  });

  it('Supports resending after worker dies', function (done) {
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

describe('Concurrency', function () {
  var bhost = location + uuid.v4();

  var broker = new PIGATO.Broker(bhost);
  broker.start(function() {});

  after(function(done) {
    broker.stop();
    done();
  });

  it('20 concurrent requests', function(done) {
    var ns = uuid.v4();
    var chunk = 'bar';
    
    this.timeout(15000);

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
