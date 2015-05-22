var zmq = require('zmq');
var _ = require('lodash');
var util = require('util');
var Base = require('./Base');

function Directory(endpoint, conf) {
  this.service = '$dir';
  this._dir = {};

  Base.call(this, endpoint, conf);
}; 
util.inherits(Directory, Base);

Directory.prototype.start = function() {
  Base.prototype.start.call(this);

  var self = this;

  this.wrk.on('request', function(inp, rep) {
    rep.end(self._dir[inp] || []);
  });

  this.sub.on('message', function(data) {
    data = data.toString();
    if (data.indexOf(self.service) === 0) {
      var msg = data.substr(5);
      msg = JSON.parse(msg);
      self._dir = msg;
    } 
  });
  
  this.sub.subscribe(this.service);
  this.onStart();
};

Directory.prototype.stop = function() {
  if (this.sub) {
    this.sub.unsubscribe(this.service);
  }

  Base.prototype.stop.call(this);
  this.onStop();
};

module.exports = Directory;
