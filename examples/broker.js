var Broker = require('./../index').Broker;

var broker = new Broker("tcp://*:55555");
broker.start(function(){});
