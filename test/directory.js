var PIGATO = require('../');
var chai = require('chai');
var uuid = require('node-uuid');

var bhost = 'inproc://#' + uuid.v4();
//var bhost = 'tcp://0.0.0.0:2020';

var broker;
var ds;
var brokerConf = {};



describe('DIRECTORY', function() {

  beforeEach(function(done) {
    brokerConf.intch = 'ipc:///tmp/' + uuid.v4();

    broker = new PIGATO.Broker(bhost, brokerConf);

    broker.conf.onStart = function() {
      ds = new PIGATO.services.Directory(bhost, {
        intch: broker.conf.intch
      });
      ds.conf.onStart = done;
      ds.start();
    };

    broker.start();
  });

  afterEach(function(done) {
    broker.conf.onStop = done;
    ds.stop();
    broker.stop();
  });

  describe('when asking with a specific service', function() {
    it('Respond with worker handling this serviceName', function(done) {
      var nsGood = uuid.v4();
      var nsBad = uuid.v4();
      var client = new PIGATO.Client(bhost);

      var workers = {};

      workers[nsGood] = [];
      workers[nsBad] = [];

      function spawn(ns) {
        var worker = new PIGATO.Worker(bhost, ns);
        worker.on('request', function(inp, rep) {
          rep.end(worker.conf.name);
        });
        worker.start();
        return worker;
      }

      client.start();

      var samples = 3;

      for (var wi = 0; wi < samples; wi++) {
        workers[nsGood].push(spawn(nsGood));
        workers[nsBad].push(spawn(nsBad));
      }

      var workerIds = workers[nsGood].map(function(wrk) {
        return wrk.conf.name;
      });

      workerIds.sort(function(a, b) {
        return a < b ? -1 : 1;
      });

      setTimeout(function() {
        client.request('$dir', nsGood)
          .on('data', function(data) {
            chai.assert.isArray(data);
            chai.assert.equal(3, data.length);
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
      }, 100);

      function stop(err) {
        workers[nsGood].forEach(function(worker) {
          worker.stop();
        });
        workers[nsBad].forEach(function(worker) {
          worker.stop();
        });
        client.stop();
        done(err);
      }
    });
  });


  describe('when asking without a specific service', function() {
    it('Respond with all workers', function(done) {
      var nsGood = uuid.v4();
      var nsBad = uuid.v4();
      var client = new PIGATO.Client(bhost);

      var workers = {};

      workers[nsGood] = [];
      workers[nsBad] = [];

      function spawn(ns) {
        var worker = new PIGATO.Worker(bhost, ns);
        worker.on('request', function(inp, rep) {
          rep.end(worker.conf.name);
        });
        worker.start();
        return worker;
      }

      client.start();

      var samples = 3;

      for (var wi = 0; wi < samples; wi++) {
        workers[nsGood].push(spawn(nsGood));
        workers[nsBad].push(spawn(nsBad));
      }

      var workerIds = workers[nsGood].map(function(wrk) {
        return wrk.conf.name;
      });

      workerIds.sort(function(a, b) {
        return a < b ? -1 : 1;
      });

      setTimeout(function() {
        client.request('$dir')
          .on('data', function(data) {
            chai.assert.isObject(data);

            chai.assert.isArray(data[nsGood]);
            chai.assert.equal(3, data[nsGood].length);
            chai.assert.isArray(data[nsBad]);
            chai.assert.equal(3, data[nsBad].length);
            chai.assert.isAbove(Object.keys(data).length, 2);
            chai.assert.equal(1, data['$dir'].length);
            chai.assert.equal(ds.wrk.conf.name, data['$dir'][
              0
            ]);


          })
          .on('error', function(err) {
            stop(err);
          })
          .on('end', function() {
            stop();
          });
      }, 10);

      function stop(err) {
        workers[nsGood].forEach(function(worker) {
          worker.stop();
        });
        workers[nsBad].forEach(function(worker) {
          worker.stop();
        });
        client.stop();
        done(err);
      }
    });
  });

});
