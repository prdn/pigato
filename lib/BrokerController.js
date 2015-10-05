'use strict';

function BrokerController() {
  this.__reqs = {};
  this.__srq = {};

  this.init();
}

BrokerController.prototype.init = function() {};

BrokerController.prototype._srq = function(srv) {
  if (this.__srq[srv]) {
    return this.__srq[srv];
  }

  var srq = this.__srq[srv] = [];
  return srq;
};

BrokerController.prototype.rset = function(req, callback) {
  this.__reqs[req.rid] = req;

  if (callback) {
    callback();
  }
};

BrokerController.prototype.rgetall = function(callback) {
  var self = this;

  setImmediate(function() {
    callback(null, self.__reqs);
  });
};

BrokerController.prototype.rget = function(rid, callback) {
  var self = this;

  setImmediate(function() {
    callback(null, self.__reqs[rid]);
  });
};

BrokerController.prototype.rdel = function(req) {
  delete this.__reqs[req.rid];
};

module.exports = BrokerController;
