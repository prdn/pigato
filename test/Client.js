var zmq = require('zmq');

var MDP = require('../lib/mdp');

var PIGATO = require('../');

var chai = require('chai'),
	assert = chai.assert;
var uuid = require('node-uuid');

var location = 'inproc://#';
var bhost = location + uuid.v4();

var client, clientOpts;

describe('Client', function() {
	var mockBroker;

	beforeEach(function() {
		bhost = location + uuid.v4();
		client = new PIGATO.Client(bhost, clientOpts);
		mockBroker = zmq.socket('router');
		mockBroker.bindSync(bhost);
	})

	afterEach(function() {
		client.stop();
		mockBroker.unbind(bhost);
	})

	it('connect to a zmq endpoint and call callback once heartbeat made round trip', function(done) {

		var called = false;

		mockBroker.on('message', function(a, b, c) {
			assert.equal(client.conf.name, a.toString());
			assert.equal(MDP.CLIENT, b.toString());
			assert.equal(MDP.W_HEARTBEAT, c.toString());

			mockBroker.send([a, b, c]);
			called = true;
		});

		client.start(function() {
			assert.equal(true, called);
			done();
		});
	});

	it('connect to a zmq endpoint and emit connect once heartbeat made round trip', function(done) {

		var called = false;

		mockBroker.on('message', function(a, b, c) {
			assert.equal(client.conf.name, a.toString());
			assert.equal(MDP.CLIENT, b.toString());
			assert.equal(MDP.W_HEARTBEAT, c.toString());

			mockBroker.send([a, b, c]);
			called = true;
		});

		client.start();
		client.on('connect', function() {
			assert.equal(true, called);
			done();
		});
	});


	it('doesn\'t call callback if no heartbeat response', function(done) {

		var called = false;
		var cbCalled = false;

		mockBroker.on('message', function(a, b, c) {
			assert.equal(client.conf.name, a.toString());
			assert.equal(MDP.CLIENT, b.toString());
			assert.equal(MDP.W_HEARTBEAT, c.toString());

			called = true;
		});

		client.start(function() {
			cbCalled = true;
		});

		setTimeout(function() {
			assert.equal(true, called);
			assert.equal(false, cbCalled);
			clientOpts
			done();
		}, 20);
	});


	it('doesn\'t call callback if answer with bad type', function(done) {

		var called = false;
		var cbCalled = false;

		mockBroker.on('message', function(a, b, c) {
			assert.equal(client.conf.name, a.toString());
			assert.equal(MDP.CLIENT, b.toString());
			assert.equal(MDP.W_HEARTBEAT, c.toString());

			mockBroker.send([a, b, MDP.W_REPLY]);
			called = true;
		});

		client.start(function() {
			cbCalled = true;
		});

		setTimeout(function() {
			assert.equal(true, called);
			assert.equal(false, cbCalled);
			done();
		}, 20);
	});

	it('emit an error if answer with bad header', function(done) {

		var called = false;
		var cbCalled = false;

		mockBroker.on('message', function(a, b, c) {
			assert.equal(client.conf.name, a.toString());
			assert.equal(MDP.CLIENT, b.toString());
			assert.equal(MDP.W_HEARTBEAT, c.toString());

			mockBroker.send([a, MDP.WORKER, c]);
			called = true;
		});

		client.on('error', function(err) {
			assert.equal('ERR_MSG_HEADER', err);
			assert.equal(true, called);
			assert.equal(false, cbCalled);
			done();
		})

		client.start(function(err) {
			cbCalled = true;
		});
	});



	it('doesn\'t call callback if answer with bad id', function(done) {

		var called = false;
		var cbCalled = false;

		mockBroker.on('message', function(a, b, c) {
			assert.equal(client.conf.name, a.toString());
			assert.equal(MDP.CLIENT, b.toString());
			assert.equal(MDP.W_HEARTBEAT, c.toString());
			mockBroker.send([a + '' +uuid.v4(), b, MDP.W_HEARTBEAT]);
			called = true;
		});

		client.start(function() {
			cbCalled = true;
		});

		setTimeout(function() {
			assert.equal(true, called);
			assert.equal(false, cbCalled);
			done();
		}, 20);
	});


	it('can do callback request with no partial', function(done) {

		var called = false;
		var cbCalled = false;

		var toAnswer = uuid.v4();

		mockBroker.on('message', function(id, clazz, type, topic, rid, data, opts) {
			if (type.toString() === MDP.W_HEARTBEAT) {
				mockBroker.send([id, clazz, MDP.W_HEARTBEAT]);
				return;
			}

			if (type.toString() === MDP.W_REQUEST) {
				mockBroker.send([id, clazz, MDP.W_REPLY, '', rid, null, JSON.stringify(toAnswer)]);
			}

		});

		var partial = false;
		client.start(function() {

			client.request('foo', 'bar', function() {
				partial = true;
			}, function(err, data) {
				assert.equal(false, partial);
				assert.equal(err, 0);
				assert.equal(data, toAnswer);
				done();
			})
		});
	});

	it('can do stream request with no partial', function(done) {

		var called = false;
		var cbCalled = false;

		var toAnswer = uuid.v4();

		mockBroker.on('message', function(id, clazz, type, topic, rid, data, opts) {
			if (type.toString() === MDP.W_HEARTBEAT) {
				mockBroker.send([id, clazz, MDP.W_HEARTBEAT]);
				return;
			}

			if (type.toString() === MDP.W_REQUEST) {
				mockBroker.send([id, clazz, MDP.W_REPLY, '', rid, null, JSON.stringify(toAnswer)]);
			}
		});

		var partial = false;
		client.start(function() {

			client.request('foo', 'bar').on('data', function(data) {
				assert.equal(data, toAnswer);
			}).on('end', function(err, data) {
				clientOpts
				done();
				clientOpts
			})
		});
	});



	it('can do stream request with partial', function(done) {

		var called = false;
		var cbCalled = false;

		var reponses = ['one', 'two', 'three'];

		mockBroker.on('message', function(id, clazz, type, topic, rid, data, opts) {
			if (type.toString() === MDP.W_HEARTBEAT) {
				mockBroker.send([id, clazz, MDP.W_HEARTBEAT]);
				return;
			}

			if (type.toString() === MDP.W_REQUEST) {
				mockBroker.send([id, clazz, MDP.W_REPLY_PARTIAL, '', rid, null, JSON.stringify(reponses[0])]);
				mockBroker.send([id, clazz, MDP.W_REPLY_PARTIAL, '', rid, null, JSON.stringify(reponses[1])]);
				mockBroker.send([id, clazz, MDP.W_REPLY, '', rid, null, JSON.stringify(reponses[2])]);
			}
		});


		var partial = false;
		var index = 0;
		client.start(function() {
			client.request('foo', 'bar').on('data', function(data) {
				assert.equal(data, reponses[index]);
				index++;

			}).on('end', function(err, data) {
				assert.equal(3, index);
				done();
			})
		});
	});

	it('can do callback request with partial', function(done) {

		var called = false;
		var cbCalled = false;

		var reponses = ['one', 'two', 'three'];

		mockBroker.on('message', function(id, clazz, type, topic, rid, data, opts) {
			if (type.toString() === MDP.W_HEARTBEAT) {
				mockBroker.send([id, clazz, MDP.W_HEARTBEAT]);
				return;
			}

			if (type.toString() === MDP.W_REQUEST) {
				mockBroker.send([id, clazz, MDP.W_REPLY_PARTIAL, '', rid, null, JSON.stringify(reponses[0])]);
				mockBroker.send([id, clazz, MDP.W_REPLY_PARTIAL, '', rid, null, JSON.stringify(reponses[1])]);
				mockBroker.send([id, clazz, MDP.W_REPLY, '', rid, null, JSON.stringify(reponses[2])]);
			}
		});

		var partial = false;
		var index = 0;
		client.start(function() {
			client.request('foo', 'bar', function(err, data) {
				assert.equal(data, reponses[index]);
				index++;
			}, function(err, data) {
				assert.equal(2, index);
				assert.equal(data, reponses[index]);
				done();
			});
		});
	});


	it('nothing append on client if we send a reply to an unknown request', function(done) {

		var clientError = false;

		client.on('error', function(err) {
			clientError = true;
		});


		mockBroker.on('message', function(id, clazz, type, topic, rid, data, opts) {
			if (type.toString() === MDP.W_HEARTBEAT) {
				mockBroker.send([id, clazz, MDP.W_HEARTBEAT]);
				return;
			}
		});
		client.start(function(err) {
			mockBroker.send([client.conf.name, MDP.CLIENT, MDP.W_REPLY, '', 'MYUNKNOWREQUEST', null, JSON.stringify("bar")]);

			setTimeout(function() {
				assert.equal(false, clientError);
				done();
			}, 20);
		});
	});


	it('emit an error if answer with bad type', function(done) {

		var called = false;
		var cbCalled = false;

		mockBroker.on('message', function(id, clazz, type, topic, rid) {
			if (type.toString() === MDP.W_HEARTBEAT) {
				mockBroker.send([id, clazz, MDP.W_HEARTBEAT]);
				return;
			}

			if (type.toString() === MDP.W_REQUEST) {
				called = true;
				mockBroker.send([id, clazz, '999', '', rid, null, JSON.stringify('DATA')]);
			}
		});

		client.on('error', function(err) {
			assert.ok(err)
			assert.equal(err, 'ERR_MSG_TYPE');
			assert.equal(true, called);
			done();
		})

		client.start(function(err) {
			client.request('foo', 'bar', function(err, data) {
				assert.ok(false);
			}, function(err, data) {
				assert.ok(false);
			});

			cbCalled = true;
		});
	});



	describe('when timeout exceeded with heartbeat short ', function() {

		before(function() {
			clientOpts = {
				heartbeat: 5
			};
		})

		it('emit an error when timeout exceeded', function(done) {

			var called = false;
			var cbCalled = false;

			mockBroker.on('message', function(id, clazz, type, topic, rid) {
				if (type.toString() === MDP.W_HEARTBEAT) {
					mockBroker.send([id, clazz, MDP.W_HEARTBEAT]);
					return;
				}
			});

			client.start(function(err) {

				client.request('foo', 'bar', function(err, data) {
					assert.ok(false);
				}, function(err, data) {
					assert.ok(err);
					assert.equal(data, undefined);
					assert.equal('C_TIMEOUT', err);
					done();

				}, {
					timeout: 10
				});
			});
		});

		after(function() {
			clientOpts = undefined;
		})
	});


	describe('when timeout exceeded with heartbeat long ', function() {

		before(function() {
			clientOpts = {
				heartbeat: 50
			};
		})

		it('wait for the heartbeat to expire before the error is returned', function(done) {

			var called = false;
			var cbCalled = false;

			mockBroker.on('message', function(id, clazz, type, topic, rid) {
				if (type.toString() === MDP.W_HEARTBEAT) {
					mockBroker.send([id, clazz, MDP.W_HEARTBEAT]);
					return;
				}
			});

			client.start(function(err) {

				client.request('foo', 'bar', function(err, data) {
					assert.ok(false);
				}, function(err, data) {
					cbCalled = true;
					assert.ok(err);
					assert.equal(data, undefined);
					assert.equal('C_TIMEOUT', err);
				}, {
					timeout: 10
				});
			});

			setTimeout(function() {
				assert.equal(false, cbCalled);
			}, 30);


			setTimeout(function() {
				assert.equal(true, cbCalled);
				done();
			}, 60);

		});

		after(function() {
			clientOpts = undefined;
		})
	});


	describe('when heartbeat is setted', function() {

		before(function() {
			clientOpts = {
				heartbeat: 10
			};
		})

		it('send heartbeat regularly', function(done) {

			var called = false;
			var cbCalled = false;
			var heartbeatCount = 0;

			mockBroker.on('message', function(id, clazz, type, topic, rid) {
				if (type.toString() === MDP.W_HEARTBEAT) {
					mockBroker.send([id, clazz, MDP.W_HEARTBEAT]);
					heartbeatCount++;
					return;
				}
			});

			client.start(function(err) {

				assert.equal(1, heartbeatCount);

				setTimeout(function() {
					assert.equal(3, heartbeatCount);
					done();
				}, 25);
			});

		});

		after(function() {
			clientOpts = undefined;
		})
	});


	it('can send heartbeat from a request, and it will send the good requestId', function(done) {

		var called = false;
		var cbCalled = false;
		var heartbeatCount = 0;

		var requestId;

		mockBroker.on('message', function(id, clazz, type, topic, rid) {
			if (type.toString() === MDP.W_HEARTBEAT && topic == undefined) {
				mockBroker.send([id, clazz, MDP.W_HEARTBEAT]);
				return;
			}

			if (type.toString() === MDP.W_REQUEST) {
				requestId = rid.toString();
			} else if (type.toString() === MDP.W_HEARTBEAT) {
				rid = topic;
				heartbeatCount++;

				assert.ok(requestId);
				assert.equal(client.conf.name, id.toString());
				assert.equal(MDP.CLIENT, clazz.toString());
				assert.equal(MDP.W_HEARTBEAT, type.toString());
				assert.equal( requestId , rid.toString())
			}

			if (heartbeatCount == 1) {
				done()
			}

		});

		client.start(function(err) {

			var req = client.request('foo', 'bar');
			req.heartbeat();
		});

	});


	it('can send manual heartbeat for an unknown request', function(done) {

		var called = false;
		var cbCalled = false;

		var heartbeatCount = 0;

		var heartbeatContent = ['ONE', 'TWO', 'THREE', 'THREE'];

		mockBroker.on('message', function(id, clazz, type, rid) {
			if (type.toString() === MDP.W_HEARTBEAT && rid == undefined) {
				mockBroker.send([id, clazz, MDP.W_HEARTBEAT]);
				return;
			}

			if (type.toString() === MDP.W_HEARTBEAT) {
				assert.equal(client.conf.name, id.toString());
				assert.equal(MDP.CLIENT, clazz.toString());
				assert.equal(MDP.W_HEARTBEAT, type.toString());
				assert.equal(heartbeatContent[heartbeatCount], rid.toString())
				heartbeatCount++;
			}

			if (heartbeatCount == 4) {
				done()
			}

		});

		client.start(function(err) {

			client.heartbeat(heartbeatContent[0]);
			client.heartbeat(heartbeatContent[1]);
			client.heartbeat(heartbeatContent[2]);
			client.heartbeat(heartbeatContent[3]);

		});

	});

})