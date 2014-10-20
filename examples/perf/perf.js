var cluster = require('cluster');
var pigato = require('../../');

var chunk = 'foo';
var wnum = +(process.argv[2] || 1);
var probes = +(process.argv[3] || 10000);

if (cluster.isMaster) {
	for (var i = 0; i < 3; i++) {
		cluster.fork();
	}
	cluster.on('exit', function(worker, code, signal) {
		for (var id in cluster.workers) {
			cluster.workers[id].kill();
		}
	});
} else {
	var workerID = cluster.worker.workerID;
	switch (+workerID) {
		case 1:
			var broker = new pigato.Broker('tcp://*:55559');
			broker.start(function() {
				console.log("BROKER started");
			});
		break;
		case 2:
			console.log(wnum + " WORKERS created");
			for (var i = 0; i < wnum; i++) {
				(function(i) {
					var worker = new pigato.Worker('tcp://127.0.0.1:55559', 'echo');
					worker.on('request', function(inp, res) {
						res.end(inp + 'FINAL');
						//console.log("WORKER " + i);
					});
					worker.start();
				})(i);
			}
		break;
		case 3:
			console.log(probes + " CLIENT requests");

			var client = new pigato.Client('tcp://127.0.0.1:55559');
			client.start();
			
			var timer = process.hrtime();
			var rcnt = 0;

			function acc() {
				rcnt++;
			  
				if (rcnt < probes) {
					return;
				}

				var elapsed = process.hrtime(timer);
				var dts = elapsed[0] + (elapsed[1] / 1000000000);
				console.log("CLIENT GOT answer", dts + " milliseconds. " + (probes / dts).toFixed(2) + " requests/sec.");
				client.stop();
				process.exit(-1);
			}

			for (var i = 0; i < probes; i++) {
				(function(i) {
					client.request(
						'echo', chunk,
						{ timeout: -1 }
					)
					.on('data', function() {})
					.on('end', function() {
						acc();
					});
				})(i);
			}
		break;
	}
}
