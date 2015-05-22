var zmq = require('zmq');
var _ = require('lodash');
var Worker = require('./../index').Worker;

function Base(endpoint, conf) {
  this.endpoint = endpoint;
  this.conf = _.extend({}, conf);
}; 

Base.prototype.start = function() {
  this.wrk = new Worker(this.endpoint, this.service);
  this.wrk.start();

  this.sub = zmq.socket('sub');
  this.sub.identity = this.wrk.name + '/sub';

  this.sub.connect(this.conf.intch);
};

Base.prototype.onStart = function() {
  if (this.conf.onStart) {
    this.conf.onStart();
  }
};

Base.prototype.stop = function() {
  if (this.wrk) {
    this.wrk.stop();
    delete this.wrk;
  }

  if (this.sub) {
    this.sub.close();
    delete this.sub;
  }
};

Base.prototype.onStop = function() {
  if (this.conf.onStop) {
    this.conf.onStop();
  }
};

module.exports = Base;
