'use strict';

require('es6-shim');

var zmq = require('zmq');
var Writable = require('readable-stream').Writable;
var debug = require('debug')('pigato:Worker');
var uuid = require('node-uuid');
var util = require('util');
var events = require('events');
var _ = require('lodash');
var MDP = require('./mdp');
var putils = require('./utils');

var HEARTBEAT_LIVENESS = 3;

function Worker(broker, service, conf) {
  this.broker = broker;
  this.service = service;

  this.conf = {
    heartbeat: 2500,
    reconnect: 1000,
    concurrency: 100,
    prefix: 'W' + uuid.v4()
  };

  _.extend(this.conf, conf);

  this.reqs = new Map();

  events.EventEmitter.call(this);
}
util.inherits(Worker, events.EventEmitter);

Worker.prototype.onConnect = function() {
  this.emit.apply(this, ['connect']);
  if (this.conf.onConnect) {
    this.conf.onConnect();
  }

  debug('W(' + this.conf.prefix + ') connected to B(%s)', this.broker);
};

Worker.prototype.onDisconnect = function() {
  this.emit.apply(this, ['disconnect']);
  if (this.conf.onDisconnect) {
    this.conf.onDisconnect();
  }

  debug('W(' + this.conf.prefix + ') disconnected from B(%s)', this.broker);
};

Worker.prototype.start = function() {
  var self = this;

  this.stop();

  this._mcnt = 0;

  this.socket = zmq.socket('dealer');

  this.socketId = this.conf.prefix + '-' + uuid.v4();
  this.socket.identity = new Buffer(this.socketId);
  this.socket.setsockopt('linger', 1);

  this.socket.on('message', function() {
    self.onMsg.call(self, putils.args(arguments));
  });

  this.socket.on('error', function(err) {
    self.emitErr(err);
  });

  this.socket.connect(this.broker);
  this.liveness = HEARTBEAT_LIVENESS;

  debug('Worker ' + this.conf.prefix + ' connected to %s', this.broker);

  this.sendReady();

  debug('W: starting');

  this.hbTimer = setInterval(function() {
    self.liveness--;

    if (self.liveness <= 0) {
      debug('W: liveness=0');
      self.stop();
      setTimeout(function() {
        self.start();
      }, self.conf.reconnect);
      return;
    }

    self.heartbeat();

    self.reqs.forEach(function(req) {
      req.liveness--;
    });
  }, this.conf.heartbeat);

  this.heartbeat();
  this.emit.apply(this, ['start']);
};

Worker.prototype.stop = function() {
  clearInterval(this.hbTimer);
  
  if (this.socket) {
    debug('W: stopping');
    
    this.sendDisconnect();
    
    var socket = this.socket;
    delete this.socket;
    delete this.socketId;

    setImmediate(function() {
      if (socket._zmq.state != zmq.STATE_CLOSED) {
        socket.close();
      }
    });

    this.onDisconnect();
    this.emit.apply(this, ['stop']);
  }
};

Worker.prototype.send = function(msg) {
  if (!this.socket) {
    return;
  }

  this.socket.send(msg);
};

// process message from broker
Worker.prototype.onMsg = function(msg) {
  this._mcnt++;

  if (this._mcnt === 1) {
    this.onConnect();
  }

  msg = putils.mparse(msg);

  var header = msg[0];
  var type = msg[1];

  if (header != MDP.WORKER) {
    this.emitErr('ERR_MSG_HEADER');
    // send error
    return;
  }

  this.liveness = HEARTBEAT_LIVENESS;

  var clientId;
  var rid;
  var service;

  if (type == MDP.W_REQUEST) {
    clientId = msg[2];
    service = msg[3];
    rid = msg[5];
    debug('W: W_REQUEST:', clientId, rid);
    this.onRequest(clientId, service, rid, msg[6], msg[7]);
  } else if (type == MDP.W_HEARTBEAT) {
    if (msg.length === 5) {
      clientId = msg[2];
      rid = msg[4];
      if (rid && this.reqs.has(rid)) {
        var req = this.reqs.get(rid);
        req.liveness = HEARTBEAT_LIVENESS;
      }
    }
  } else if (type == MDP.W_DISCONNECT) {
    debug('W: W_DISCONNECT');
    this.start();
  } else if (type == MDP.W_READY) {
    debug('W: W_READY');
  } else {
    this.emitErr('ERR_MSG_TYPE_INVALID');
  }
};

Worker.prototype.emitReq = function(req, rep) {
  this.emit.apply(this, ['request', req.data, rep, req.opts]);
};

Worker.prototype.emitErr = function(msg) {
  this.emit.apply(this, ['error', msg]);
};

Worker.prototype.onRequest = function(clientId, service, rid, data, opts) {
  var self = this;

  var req = {
    clientId: clientId,
    rid: rid,
    liveness: HEARTBEAT_LIVENESS,
    service: service,
    data: null,
    opts: {} 
  };

  try { req.data = JSON.parse(data); } catch(e) {
    // Ignore
  }
  try { req.opts = JSON.parse(opts); } catch(e) {
    // Ignore
  }

  this.reqs.set(rid, req);

  var reply = new Writable({
    objectMode: true
  });

  reply.ended = false;

  var _write = reply.write;
  reply.write = function(chunk, encoding, cb) {
    return _write.call(reply, chunk, encoding, cb);
  };

  reply._write = function(chunk, encoding, cb) {
    var rf = self.replyPartial;
    if (this.ended) {
      rf = self.replyFinal;
    }
    if (this.endNull) {
      chunk = null;
    }
    rf.apply(self, [clientId, rid, chunk, reply.opts]);
    cb(null);
  };

  reply.opts = {};

  reply.active = function() {
    return self.reqs.has(rid) && !req.ended && req.liveness > 0;
  };

  reply.heartbeat = function() {
    self.heartbeat();
  };

  var _end = reply.end;

  reply.end = function(chunk, encoding, cb) {
    this.ended = true;

    if (chunk === undefined || chunk === null) {
      chunk = '';
      this.endNull = true;
    }

    var ret = _end.apply(reply, [chunk, encoding, cb]);

    self.dreq(rid);
    return ret;
  };

  reply.reject = function(err) {
    self.replyReject(clientId, rid, err);
    self.dreq(rid);
  };

  reply.error = function(err) {
    self.replyError(clientId, rid, err);
    self.dreq(rid);
  };

  this.emitReq(req, reply);
};

Worker.prototype.dreq = function(rid) {
  this.reqs.delete(rid);
};

Worker.prototype.sendReady = function() {
  this.send([MDP.WORKER, MDP.W_READY, this.service]);
};

Worker.prototype.sendDisconnect = function() {
  this.send([MDP.WORKER, MDP.W_DISCONNECT]);
};

Worker.prototype.heartbeat = function(opts) {
  this.send([
    MDP.WORKER, MDP.W_HEARTBEAT, '',
    JSON.stringify(_.extend({}, opts, {
      concurrency: this.conf.concurrency
    }))
  ]);
};

Worker.prototype.reply = function(type, clientId, rid, code, data, opts) {
  this.send([MDP.WORKER, type, clientId, '', rid, code, JSON.stringify(data), JSON.stringify(opts)]);
};

Worker.prototype.replyPartial = function(clientId, rid, data, opts) {
  this.reply(MDP.W_REPLY_PARTIAL, clientId, rid, 0, data, opts);
};

Worker.prototype.replyFinal = function(clientId, rid, data, opts) {
  this.reply(MDP.W_REPLY, clientId, rid, 0, data, opts);
};

Worker.prototype.replyReject = function(clientId, rid, err) {
  this.reply(MDP.W_REPLY_REJECT, clientId, rid, 0, err);
};

Worker.prototype.replyError = function(clientId, rid, err) {
  this.reply(MDP.W_REPLY, clientId, rid, -1, err);
};

module.exports = Worker;
