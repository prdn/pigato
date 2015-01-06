var fs = require('fs');
var Broker = require('./../index').Broker;

var conf = JSON.parse(fs.readFileSync(__dirname + '/config.json', 'UTF-8'));

var broker = new Broker("tcp://*:" + conf.broker.port);
broker.start(function() {});
