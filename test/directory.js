var PIGATO = require('../');
var chai = require('chai');
var uuid = require('node-uuid');

var location = 'inproc://#';

var bhost = location + uuid.v4();

var broker = new PIGATO.Broker(bhost);
var ds;
describe('DIRECTORY', function() {

  before(function(done) {
    broker.conf.onStart = function() {
      ds = new PIGATO.services.Directory(bhost, {
        intch: broker.conf.intch
      });
      ds.conf.onStart = done;
      ds.start();
    }
    broker.start();
  });

  after(function(done) {
    broker.conf.onStop = done;
    ds.stop();
    broker.stop();
  });

  it('Base', function(done) {
    var ns = uuid.v4();
    var client = new PIGATO.Client(bhost);

    var workers = [];

    function spawn() {
      var worker = new PIGATO.Worker(bhost, ns);
      worker.on('request', function(inp, rep) {
        rep.end(worker.conf.name);
      });
      worker.start();
      workers.push(worker);
    };

    client.start();

    var samples = 3;

    for (var wi = 0; wi < samples; wi++) {
      spawn();
    }

    var workerIds = workers.map(function(wrk) {
      return wrk.conf.name;
    });

    workerIds.sort(function(a, b) {
      return a < b ? -1 : 1;
    });

    setTimeout(function() {
      client.request('$dir', ns)
        .on('data', function(data) {
          chai.assert.isArray(data);
          data.sort(function(a, b) {
            return a < b ? -1 : 1;
          });

          chai.assert.deepEqual(data, workerIds);
        })
        .on('error', function(err) {
          stop(err);
        })
        .on('end', function() {
          stop();
        });
    }, 10);

    function stop(err) {
      workers.forEach(function(worker) {
        worker.stop();
      });
      client.stop();
      done(err);
    }
  })
});
