var PIGATO = require('../');

var chai = require('chai'),
  assert = chai.assert;
var uuid = require('node-uuid');

//var bhost = 'inproc://#' + uuid.v4();
var bhost = 'tcp://0.0.0.0:2020';

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
      broker.conf.onStart = done;
      broker.start();
    });

    it('call the callback on stop', function(done) {
      broker.conf.onStop = done;
      broker.stop();
    });
  });

  describe('When I start a worker against a running broker', function() {

    before(function(done) {
      broker.conf.onStart = done;
      broker.start();
    });


    it('call the callback on start', function(done) {
      worker.conf.onConnect = done;
      worker.start();
    });

    it('call the callback on stop', function(done) {
      worker.conf.onDisconnect = done;
      worker.stop();
    });

    it('call the callback on (re)start', function(done) {
      worker.conf.onConnect = done;
      worker.start();
    });

    it('call the callback on (re)stop', function(done) {
      worker.conf.onDisconnect = done;
      worker.stop();
    });

    after(function(done) {
      broker.conf.onStop = done;
      broker.stop();
      worker.conf.onConnect = null;
      worker.conf.onDisconnect = null;
    });
  });


  describe('When I start a client against a running broker', function() {

    before(function(done) {
      broker.conf.onStart = done;
      broker.start();
    });

    it('call the callback on start', function(done) {
      client.conf.onConnect = done;
      client.start();
    });

    it('call the callback on stop', function(done) {
      client.conf.onDisconnect = done;
      client.stop();
    });

    it('call the callback on (re)start', function(done) {
      client.conf.onConnect = done;
      client.start();
    });

    it('call the callback on (re)stop', function(done) {
      client.conf.onDisconnect = done;
      client.stop();
    });

    after(function(done) {
      broker.conf.onStop = done;
      broker.stop();
    });

  });


  describe('When I start a client and a worker against a running broker', function() {

    before(function(done) {
      broker.conf.onStart = done;
      broker.start();
    });

    it('call the callback on start', function(done) {
      worker.conf.onConnect = function() {
        client.conf.onConnect = done;
        client.start();
      };
      worker.start();
    });

    it('call the callback on stop', function(done) {
      worker.conf.onDisconnect = function() {
        client.conf.onDisconnect = done;
        client.stop();
      };
      worker.stop();
    });


    after(function(done) {
      broker.conf.onStop = done;
      broker.stop();
    });
  });
});
