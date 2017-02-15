var Broker = require('./../index').Broker;
var conf = require('./config.json');

var broker = new Broker(conf.broker.host);
broker.start();
