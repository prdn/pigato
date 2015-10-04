var PIGATO = require('../');
var zmq = require('zmq');
var chai = require('chai');
var assert = chai.assert;

//var bhost = 'inproc://#' + uuid.v4();
var bhost = 'tcp://0.0.0.0:2020';

var broker = new PIGATO.Broker(bhost);

describe('FILE DESCRIPTORS', function() {
  before(function(done) {
    broker.conf.onStart = done;
    broker.start();
  });

  after(function(done) {
    broker.conf.onStop = done;
    broker.stop();
  });

  var spawn = function(hm, callback) {
    var clients = [];
    try {
      for (var ci = 0; ci < hm; ci++) {
        var c = new PIGATO.Client(bhost);
        c.start();
        clients.push(c);
      }
    } catch (err) {
      return callback(err, clients);
    }

    callback(null, clients);
  };

/*  describe("When I create too many sockets", function() {
    it('return a \'Too many open files\' error', function(done) {
      spawn(zmq.Context.getMaxSockets() * 2, function(err, clients) {

        assert.ok(err);
        assert.ok(err.message);
        assert.equal('Too many open files', err.message);
        clients.forEach(function(client) {
          client.stop();
        });
        setTimeout(done, 100);
      });
    });
  });*/


  describe('When I create lots of sockets', function() {

    before(function(done) {
      setTimeout(done, 100);
    });

    it('still works if I close them in the meantime', function(done) {

      var cnt = 0;
      var step = function() {
        spawn(zmq.Context.getMaxSockets() - 100, function(err, clients) {
          assert.ok(!err);
          clients.forEach(function(client) {
            client.stop();
            cnt++;
          });

          setImmediate(next);
        });
      };

      var next = function() {
        if (cnt < zmq.Context.getMaxSockets() * 5) {
          setTimeout(step, 30);
        } else {
          done();
        }
      };
      next();
    });
  });
});
