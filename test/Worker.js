var zmq = require('zmq');

var MDP = require('../lib/mdp');

var PIGATO = require('../');

var chai = require('chai'),
	assert = chai.assert;
var uuid = require('node-uuid');

var location = 'inproc://#';
var bhost = location + uuid.v4();

var worker, workerOpts, workerTopic = 'worker';

describe('Worker', function() {
	var mockBroker;

	beforeEach(function() {
		bhost = location + uuid.v4();
		worker = new PIGATO.Worker(bhost, workerTopic, workerOpts);
		mockBroker = zmq.socket('router');
		mockBroker.bindSync(bhost);
	})

	afterEach(function(done) {
		mockBroker.unbind(bhost);
		worker.stop(done);
	})

	it('connect to a zmq endpoint and call callback once ready made round trip', function(done) {

		var called = false;

		var types = [MDP.W_READY, MDP.W_DISCONNECT];

		var typesIndex = 0;
		mockBroker.on('message', function(a, b, c) {
			assert.equal(worker.conf.name, a.toString());
			assert.equal(MDP.WORKER, b.toString());
			assert.equal(types[typesIndex], c.toString());
			typesIndex++;
			mockBroker.send([a, b, c]);
			called = true;
		});

		worker.start(function() {
			assert.equal(true, called);
			done();
		});
	});

	it('connect to a zmq endpoint and emit \'connect\' once ready made round trip', function(done) {

		var called = false;

		var types = [MDP.W_READY, MDP.W_DISCONNECT];

		var typesIndex = 0;
		mockBroker.on('message', function(a, b, c) {
			assert.equal(worker.conf.name, a.toString());
			assert.equal(MDP.WORKER, b.toString());
			assert.equal(types[typesIndex], c.toString());
			typesIndex++;
			mockBroker.send([a, b, c]);
			called = true;
		});

		worker.on('connect', function() {
			assert.equal(true, called);
			done();
		});
		worker.start();
	});


	describe('emit hearbeat regularly', function() {


		before(function() {
			workerOpts = {
				heartbeat: 10
			}
		});

		after(function() {
			workerOpts = undefined;
		});

		it('emit hearbeat regularly ', function(done) {

			var heartbeatCount = 0
			var typesIndex = 0;
			var lastDate = 0;

			mockBroker.on('message', function(a, b, c) {
				assert.equal(worker.conf.name, a.toString());
				assert.equal(MDP.WORKER, b.toString());

				mockBroker.send([a, b, c]);
				if (c.toString() == MDP.W_HEARTBEAT) {
					heartbeatCount++;

					var currentDate = +new Date();
					console.log("data", currentDate, lastDate, currentDate - lastDate)

					// should be around 10, but jitter can occur
					assert.ok(currentDate - lastDate > 5 );
					assert.ok(currentDate - lastDate < 25);
					lastDate = currentDate;

					if (heartbeatCount == 3) {
						done();
					}
				}
			});


			worker.start(function() {
				lastDate = +new Date();
			
				console.log("worker started", worker.conf)
				
			});
		});

	})
})