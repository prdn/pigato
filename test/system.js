var PIGATO = require('../');
var zmq = require('zmq');
var chai = require('chai');
var uuid = require('node-uuid');

var location = 'inproc://#';

describe('FILE DESCRIPTORS', function() {
  var bhost = location + uuid.v4();

  var broker = new PIGATO.Broker(bhost)
  broker.start(function() {});

  after(function(done) {
    broker.stop();
    setTimeout(function() {
      done();
    }, 1000);
  });

  it('Client flood', function(done) {
    this.timeout(5 * 1000);

    var cnt = 0;

    function spawn(hm) { 
      for (var ci = 0; ci < hm; ci++) {
        var c = new PIGATO.Client(bhost);
        c.start();
        c.stop();
        cnt++
      }
    }

    var itv = setInterval(function() {
      if (cnt > 5000) {
        clearInterval(itv);
        done();
        return;
      }
      spawn(zmq.Context.getMaxSockets() - 100);
    }, 100);
  });
});
