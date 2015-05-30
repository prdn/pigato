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
    mockBroker.removeAllListeners('message');

    worker.on('disconnect', function() {
      done();
    });

    worker.stop();
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
      if (typesIndex == 1) {
        mockBroker.send([a, b, c]);
      };
      called = true;
    });


    worker.conf.onConnect = function() {
      assert.equal(true, called);
      delete worker.conf.onConnect;
      done();
    };

    worker.start();
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

  it('emit \'connect\' at reception of first ZMQ message (even if it is not a READY message)', function(done) {

    var countMessage = 0;

    worker.on('connect', function() {
      setTimeout(function() {
        assert.equal(1, countMessage);
        done();
      }, 20);
    });

    worker.start();
    
    mockBroker.send([worker.conf.name, MDP.WORKER, MDP.W_REQUEST]);

    worker.socket.on('message', function(a, b, c) {
      assert.equal(MDP.W_REQUEST, b.toString())
      countMessage++
    });
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

          // should be around 10, but jitter can occur
          assert.ok(currentDate - lastDate > 5);
          assert.ok(currentDate - lastDate < 25);
          lastDate = currentDate;

          if (heartbeatCount == 3) {
            done();
          }
        }
      });

      worker.on('connect', function() {
        lastDate = +new Date();
      });

      worker.start();
    });

  });


  it('emit an error events when receiving a request with CLIENT as header', function(done) {

    worker.on('error', function(err) {
      assert.equal(err, 'ERR_MSG_HEADER');
      done();
    });

    worker.start();

    mockBroker.send([worker.conf.name, MDP.CLIENT, MDP.W_REQUEST]);
  });


  it('emit request events with no data when receiving a request with nothing', function(done) {

    worker.on('request', function(data, reply) {
      assert.equal(data, undefined);
      done();
    });

    worker.start();

    mockBroker.send([worker.conf.name, MDP.WORKER, MDP.W_REQUEST]);
  });

  it('emit request events with no data when receiving a request with a JSON String', function(done) {

    worker.on('request', function(data, reply) {
      assert.equal(data, 'foo');
      done();
    });

    worker.start();

    mockBroker.send([worker.conf.name, MDP.WORKER, MDP.W_REQUEST, 'clientId', workerTopic, '', 'requestId', '"foo"']);
  });

  it('emit request events with no data when receiving a request with an empty JSON object', function(done) {

    worker.on('request', function(data, reply) {
      assert.equal(typeof data, 'object');
      assert.equal(Object.keys(data).length, 0);
      done();
    });

    worker.start();

    mockBroker.send([worker.conf.name, MDP.WORKER, MDP.W_REQUEST, 'clientId', 'service', '', 'requestId', JSON.stringify({})]);
  });

  it('emit request events with no data when receiving a request with an complexe JSON object', function(done) {

    worker.on('request', function(data, reply) {
      assert.equal(typeof data, 'object');
      assert.equal(Object.keys(data).length, 4);
      assert.equal(data.foo, 'bar');
      assert.equal(data.foo, 'bar');
      assert.equal(data.life, 42);
      assert.equal(typeof data, 'object');
      assert.ok(data.obj);
      assert.equal(data.obj.foo, 'baz');
      done();
    });

    worker.start();

    mockBroker.send([worker.conf.name, MDP.WORKER, MDP.W_REQUEST, 'clientId', 'service', '', 'requestId', JSON.stringify({
      foo: 'bar',
      life: 42,
      tables: [],
      obj: {
        foo: 'baz'
      }
    })]);
  });


  it('messages sended to another worker is not received/handled', function(done) {

    var received = false;
    
    worker.on('request', function(data, reply) {
      received = true
    });

    worker.start();

    mockBroker.send([worker.conf.name + 'LLLLL', MDP.WORKER, MDP.W_REQUEST, 'clientId', 'service', '', 'requestId']);

    setTimeout(function() {
      assert.equal(received, false);
      done();
    }, 25);
  });


  describe('For an request exchange', function() {
    var toCheck = undefined;
    var toAnswer = 'HELLO';
    beforeEach(function() {
      worker.on('request', function(data, reply) {
        reply.end(toAnswer);
      });
    
      mockBroker.on('message', function(a, side, type, service, empty, rid, status, data) {
        if (MDP.WORKER == side.toString() && type.toString() == MDP.W_REPLY) {
          assert.ok(toCheck);
          toCheck.apply(toCheck, arguments);
        }
      });

      worker.start();
    });

    it('response keep the same clientId', function(done) {
      toCheck = function(a, side, type, clientId) {
        assert.ok(clientId);
        assert.equal(clientId.toString(), 'clientId');
        done()
      };
      mockBroker.send([worker.conf.name, MDP.WORKER, MDP.W_REQUEST, 'clientId', 'service', '', 'requestId']);
    });

    it('empty is empty string ', function(done) {
      toCheck = function(a, side, type, clientId, empty) {
        assert.ok(empty);
        assert.equal(empty.length, 0);
        assert.equal(empty.toString().length, 0);
        done()
      };
      mockBroker.send([worker.conf.name, MDP.WORKER, MDP.W_REQUEST, 'clientId', 'service', '', 'requestId']);
    });

    it('empty is empty string event when not empty was sended', function(done) {
      toCheck = function(a, side, type, clientId, empty) {
        assert.ok(empty);
        assert.equal(empty.length, 0);
        assert.equal(empty.toString().length, 0);
        done()
      };
      mockBroker.send([worker.conf.name, MDP.WORKER, MDP.W_REQUEST, 'clientId', 'service', 'NOTEMPTY', 'requestId']);
    });

    it('response keep the same requestId', function(done) {
      var rid = Math.random();
      toCheck = function(a, side, type, clientId, service, requestId) {
        assert.ok(requestId);
        assert.equal(requestId.toString(), 'requestId' + rid);
        done()
      };
      mockBroker.send([worker.conf.name, MDP.WORKER, MDP.W_REQUEST, 'clientId', 'service', '', 'requestId' + rid]);
    });

    it('answer status 0 for a response with no problem', function(done) {
      var rid = Math.random();
      toCheck = function(a, side, type, clientId, service, requestId, status) {
        assert.ok(status);
        assert.equal(status.toString(), 0);
        done()
      };
      mockBroker.send([worker.conf.name, MDP.WORKER, MDP.W_REQUEST, 'clientId', 'service', '', 'requestId']);
    });

    it('string data are correctly sended', function(done) {
      var rid = Math.random();
      toCheck = function(a, side, type, clientId, service, requestId, status, data) {
        assert.ok(data);
        assert.ok(data.toString());
        assert.equal(JSON.parse(data.toString()), toAnswer);
        done()
      };
      mockBroker.send([worker.conf.name, MDP.WORKER, MDP.W_REQUEST, 'clientId', 'service', '', 'requestId']);
    });

    it('object data are correctly sended', function(done) {
      
      toAnswer = {
        foo: 'bar',
        toto: 42
      };

      var rid = Math.random();
      toCheck = function(a, side, type, clientId, service, requestId, status, data) {
        assert.ok(data);
        assert.ok(data.toString());
        var parsed = JSON.parse(data.toString());
        assert.ok(parsed);
        assert.equal(Object.keys(parsed).length, 2);

        assert.equal(parsed.foo, 'bar');
        assert.equal(parsed.toto, 42);
        done()
      };
  
      mockBroker.send([worker.conf.name, MDP.WORKER, MDP.W_REQUEST, 'clientId', 'service', '', 'requestId']);
    });
  });


  describe('when I set Concurency', function() {

    before(function() {
      workerOpts = {
        concurrency: Math.floor(Math.random() * 100),
        heartbeat: 10,
        reconnect: 10
      };
    });

    it('send the defined concurrency in the hearbeat message', function(done) {

      var finished = false;

      mockBroker.on('message', function(a, b, c, empty, data) {
        assert.equal(worker.conf.name, a.toString());
        assert.equal(MDP.WORKER, b.toString());

        if (c.toString() == MDP.W_HEARTBEAT && !finished) {

          assert.ok(empty);
          assert.equal(0, empty.toString().length);

          assert.ok(data);
          var parsedConf = JSON.parse(data);
          assert.ok(parsedConf);
          assert.equal(parsedConf.concurrency, workerOpts.concurrency);

          finished = true;
          done();
        }
      });

      worker.start();
    });

    it('is not impacted by the current requests', function(done) {

      var heartbeatCount = 0;

      mockBroker.on('message', function(a, b, c, empty, data) {
        assert.equal(worker.conf.name, a.toString());
        assert.equal(MDP.WORKER, b.toString());

        if (c.toString() == MDP.W_HEARTBEAT) {
          mockBroker.send([a, b, c]);

          assert.ok(empty);
          assert.equal(0, empty.toString().length);

          assert.ok(data);
          var parsedConf = JSON.parse(data);
          assert.ok(parsedConf);
          assert.equal(parsedConf.concurrency, workerOpts.concurrency);
          heartbeatCount++;
        
          if (heartbeatCount == 3) {
            done();
          }
        }
      });

      worker.start();
      mockBroker.send([worker.conf.name, MDP.WORKER, MDP.W_REQUEST, 'clientId', 'service', '', 'requestId' + Math.floor(Math.random() * 100)]);
      mockBroker.send([worker.conf.name, MDP.WORKER, MDP.W_REQUEST, 'clientId', 'service', '', 'requestId' + Math.floor(Math.random() * 100)]);
      mockBroker.send([worker.conf.name, MDP.WORKER, MDP.W_REQUEST, 'clientId', 'service', '', 'requestId' + Math.floor(Math.random() * 100)]);
    });

  });


  describe('when I send more request in // than conf.concurency', function() {

    before(function() {
      workerOpts = {
        concurrency: 1,
        heartbeat: 10,
        reconnect: 10
      };
    });

    beforeEach(function() {
      var toAnswer = 'HELLO';

      var requestIndex = 0;
      worker.on('request', function(data, reply) {
        requestIndex++;
        setTimeout(function() {
          //we answer in reversed order to 3 first request
          reply.end(toAnswer);
        }, 30 - requestIndex * 10);
      });

      mockBroker.on('message', function(a, side, type, service, empty, rid, status, data) {
        if (MDP.WORKER == side.toString() && type.toString() == MDP.W_REPLY) {
          assert.ok(toCheck);
          toCheck.apply(toCheck, arguments);
        }
      });
    });

    after(function() {
      workerOpts = {};
    })

    it('requests are still handled in //', function(done) {

      var replyIndex = 3;
      toCheck = function(a, side, type, clientId, service, requestId, status, data) {
        assert.equal(worker.conf.name, a.toString());
        assert.equal(MDP.WORKER, side.toString());

        if (type.toString() == MDP.W_HEARTBEAT) {
          mockBroker.send([a, b, type]);

        } else if (type.toString() == MDP.W_REPLY) {

          assert.equal(requestId, '' + replyIndex);
          replyIndex--;
          if (replyIndex == 0) {
            done();
          }
        }
      };

      worker.start();

      mockBroker.send([worker.conf.name, MDP.WORKER, MDP.W_REQUEST, 'clientId', 'service', '', '1']);
      mockBroker.send([worker.conf.name, MDP.WORKER, MDP.W_REQUEST, 'clientId', 'service', '', '2']);
      mockBroker.send([worker.conf.name, MDP.WORKER, MDP.W_REQUEST, 'clientId', 'service', '', '3']);
    });

  });
});



describe('Worker Disconnection', function() {

  var mockBroker;

  beforeEach(function() {
    bhost = location + uuid.v4();
    worker = new PIGATO.Worker(bhost, workerTopic, workerOpts);
    mockBroker = zmq.socket('router');
    mockBroker.bindSync(bhost);
  });

  afterEach(function(done) {
    mockBroker.unbind(bhost);
    mockBroker.removeAllListeners('message');
    done();
  });

  
  describe('when stop is called when connected', function() {
    it('send Disconnect', function(done) {

      var types = [MDP.W_READY, MDP.W_DISCONNECT];
      var typesIndex = 0;
      
      mockBroker.on('message', function(a, b, c) {
        assert.equal(worker.conf.name, a.toString());
        assert.equal(MDP.WORKER, b.toString());
        assert.equal(types[typesIndex], c.toString());
        typesIndex++;

        if (c.toString() == MDP.W_DISCONNECT) {
          done();
          return;
        }

        setTimeout(function() {
          mockBroker.send([a, b, c]);
        }, 10);
      });

      worker.on('connect', function() {
        worker.stop();
      });

      worker.start();
    });
  });


  describe('when Broker doesn\'t anwser to heartbeat', function() {

    before(function() {
      workerOpts = {
        heartbeat: 20,
        reconnect: 200
      };
    });

    it('send 3 Heartbeat messages then reconnect', function(done) {

      var types = [MDP.W_READY, MDP.W_HEARTBEAT, MDP.W_HEARTBEAT, MDP.W_DISCONNECT, MDP.W_READY, MDP.W_HEARTBEAT];
      var typesIndex = -1;

      mockBroker.on('message', function(a, b, c) {
        typesIndex++;

        if (typesIndex == types.length) {
          done();
          return;
        }
        
        if (typesIndex >= types.length) {
          return;
        }

        assert.equal(worker.conf.name, a.toString());
        assert.equal(MDP.WORKER, b.toString());
        assert.equal(types[typesIndex], c.toString());
      });

      worker.start();
    });


    it('emit Disconnect when detecting it, after sending it', function(done) {

      var types = [MDP.W_READY, MDP.W_HEARTBEAT, MDP.W_HEARTBEAT, MDP.W_DISCONNECT];
      var typesIndex = -1;

      mockBroker.on('message', function(a, b, c) {
        typesIndex++;

        if (typesIndex >= types.length) {
          return;
        }

        assert.equal(worker.conf.name, a.toString());
        assert.equal(MDP.WORKER, b.toString());
        assert.equal(types[typesIndex], c.toString());
      });

      worker.on('disconnect', function() {
        assert(typesIndex, 3);
        worker.removeAllListeners('disconnect');
        done();
      })

      worker.start();
    });

  });
});
