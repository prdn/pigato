var cluster = require('cluster');
var pigato = require('../../');
var async = require('async');
var _ = require('lodash');

var chunk = 'foo';
var probes = 40000;
var bnum = 2;

if (cluster.isMaster) {
  for (var i = 0; i < bnum * 2 + 1; i++) {
    cluster.fork();
  }
  cluster.on('exit', function(worker, code, signal) {
    for (var id in cluster.workers) {
      cluster.workers[id].kill();
    }
  });
} else {
  var workerID = cluster.worker.id;
  if (workerID <= bnum) {
    var broker = new pigato.Broker('tcp://*:5555' + workerID);
    broker.start(function() {
      console.log("BROKER started " + workerID);
    });
  } else if (workerID <= bnum * 2) {
    var b = (workerID % bnum) + 1;
    var worker = new pigato.Worker('tcp://127.0.0.1:5555' + b, 'echo');
    worker.on('request', function(inp, res) {
      res.opts.cache = 20000;
      res.end(inp + 'FINAL');
    });
    worker.start();
    console.log("WORKER STARTED FOR BROKER ", b);
  } else if (workerID === bnum * 2 + 1) {
    console.log(probes + " CLIENT requests");
    var bs = [];
    for (var i = 0; i < bnum; i++) {
      bs.push('tcp://127.0.0.1:5555' + (i + 1));
    }
    var client = new pigato.Client(bs);
    client.start();

    var timer;
    var rcnt = 0;

    function done() {
      var elapsed = process.hrtime(timer);
      var dts = elapsed[0] + (elapsed[1] / 1000000000);
      console.log("CLIENT GOT answer", dts + " milliseconds. " + (probes / dts).toFixed(2) + " requests/sec.");
      client.stop();
      process.exit(-1);
    }

    function send() {
      for (var k = 0; k < probes; k++) {
        client.request(
          'echo', chunk + (k % 1000),
          { timeout: -1 }
        )
        .on('data', function() {})
        .on('end', function() {
          rcnt++;

          if (rcnt < probes) {
            return;
          }

          done();
        });
      }
    }

    setTimeout(function() {
      timer = process.hrtime();
      send();
    }, 1000);
  }
}
