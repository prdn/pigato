var PIGATO = require('../');

var chai = require('chai'),
  assert = chai.assert;
var uuid = require('node-uuid');


var location = 'inproc://#';

var bhost = location + uuid.v4();

var broker = new PIGATO.Broker(bhost, {
  heartbeat: 10000
});
var worker = new PIGATO.Worker(bhost, 'foo', {
  heartbeat: 10000
});

var client = new PIGATO.Client(bhost);


describe('StartStop', function() {

  describe('A broker', function() {
    it('call the callback on start', function(done) {
      broker.start(done);
    });

    it('call the callback on stop', function(done) {
      broker.stop(done);
    });
  });

  describe('When I start a worker against a running broker', function() {

    before(function(done) {
      broker.start(done);
    });


    it('call the callback on start', function(done) {
      worker.start(done);
    });


    it('call the callback on stop', function(done) {
      worker.stop(done);
    });

    it('call the callback on (re)start', function(done) {
      worker.start(done);
    });

    it('call the callback on (re)stop', function(done) {
      worker.stop(done);
    });

    after(function(done) {
      broker.stop(done);
    });

  });



  describe('When I start a client against a running broker', function() {

    before(function(done) {
      broker.start(done);
    });

    it('call the callback on start', function(done) {
      client.start(done);
    });


    it('call the callback on stop', function(done) {
      client.stop(done);
    });

    it('call the callback on (re)start', function(done) {
      client.start(done);
    });

    it('call the callback on (re)stop', function(done) {
      client.stop(done);
    });


    after(function(done) {
      broker.stop(done);
    });

  });


  describe('When I start a client and a worker against a running broker', function() {

    before(function(done) {
      broker.start(done);
    });

    it('call the callback on start', function(done) {
      worker.start(function() {
        client.start(done)
      });
    });

    it('call the callback on stop', function(done) {
      worker.stop(function() {
        client.stop(done)
      });
    });


    after(function(done) {
      broker.stop(done);
    });
  });
});
/*broker.start(function() {
  console.log("started");
  worker.start(function() {
    console.log("worker started");
    client.start(function() {
      console.log("client started");

      worker.on('request', function(res, reply) {
        reply.end('KKKK');
      });


      client.request("foo", "bar", function(err, data) {
        console.log("client requestes", err, data);
      }, function(err, data) {
        console.log("client requestes end", err, data);

        worker.stop(function() {
          console.log("worker stopped");
          client.stop(function() {
            console.log("client stopped");
            broker.stop(function(){
              console.log("broker stopped, will exit");
            })
          });
        })
      });
    });
  });
});
*/