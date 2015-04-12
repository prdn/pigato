var zmq = require('zmq');
var _ = require('lodash');
var Worker = require('./../index').Worker;

function Directory(endpoint, conf) {
  this.endpoint = endpoint;

  this.conf = _.extend({
    local: 'inproc://#pigato'
  }, conf);

  this.wrk = new Worker(this.endpoint, '$dir');
  this.services = {};

  var self = this;

  this.wrk.on('request', function(inp, rep) {
    rep.end(self.services[inp] || []);
  });
}; 

Directory.prototype.start = function() {
  this.wrk.start();

  this.sub = zmq.socket('sub');
  this.sub.identity = new Buffer(this.wrk.name + '/sub');

  this.sub.connect(this.conf.local);

  var self = this;

  this.sub.on('message', function(data) {
    data = data.toString(); 
    if (data.indexOf('$dir') === 0) {
      var msg = data.substr(5);
      msg = JSON.parse(msg);
      self.services = msg;
    } 
  });
  
  this.sub.subscribe('$dir ');
};

Directory.prototype.stop = function() {
  this.wrk.stop();

  if (this.sub) {
    this.sub.close();
    delete this.sub;
  }
};

module.exports = Directory;
