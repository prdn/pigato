var zmq = require('zmq');
var Writable = require('readable-stream').Writable
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
    name: 'W' + uuid.v4()
  };

  _.extend(this.conf, conf);

  events.EventEmitter.call(this);
}
util.inherits(Worker, events.EventEmitter);

Worker.prototype.start = function() {
  this.connectToBroker();
};

Worker.prototype.stop = function() {
  clearInterval(this.hbTimer);

  if (this.socket) {
    this.sendDisconnect();
    if(this.socket._zmq.state != zmq.STATE_CLOSED) {
      this.socket.close();
    }
    delete this['socket'];
  }
};

// Connect or reconnect to broker
Worker.prototype.connectToBroker = function() {
  var self = this;

  this.stop();

  this.socket = zmq.socket('dealer');
  this.socket.identity = new Buffer(this.conf.name);

  this.reqs = {};

  this.socket.on('message', function() {
    self.onMsg.call(self, putils.args(arguments));
  });

  this.socket.on('error', function(err) {
    self.emitErr(err); 
  });

  this.socket.connect(this.broker);

  debug('Worker ' + this.conf.name + ' connected to %s', this.broker);

  this.sendReady();
  this.liveness = HEARTBEAT_LIVENESS;

  this.hbTimer = setInterval(function() {
    self.liveness--;

    if (self.liveness <= 0) {
      debug('W: liveness=0');
      setTimeout(function() {
        self.connectToBroker();
      }, self.conf.reconnect);
      return;
    }

    self.heartbeat();

    _.each(self.reqs, function(req) {
      req.liveness--;
    });
  }, this.conf.heartbeat);
};

Worker.prototype.send = function(msg) {
  if (!this.socket) {
    return;
  }

  this._hbcheck = (new Date()).getTime();
  this.socket.send(msg);
};

// process message from broker
Worker.prototype.onMsg = function(msg) {
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
    this.onRequest(clientId, service, rid, msg[6]);
  } else if (type == MDP.W_HEARTBEAT) {
    if (msg.length === 5) {
      clientId = msg[2];
      rid = msg[4];
      if (rid && this.reqs[rid]) {
        this.reqs[rid].liveness = HEARTBEAT_LIVENESS;
      }
    }
  } else if (type == MDP.W_DISCONNECT) {
    debug('W: W_DISCONNECT');
    this.connectToBroker();
  } else {
    this.emitErr('ERR_MSG_TYPE_INVALID');
  }

  this.heartbeat();
};

Worker.prototype.emitReq = function(input, reply) {
  this.emit.apply(this, ['request', input, reply]);
};

Worker.prototype.emitErr = function(msg) {
  this.emit.apply(this, ['error', msg]);
};

Worker.prototype.onRequest = function(clientId, service, rid, data) {
  var self = this;

  var req = { 
    clientId: clientId, 
    rid: rid, 
    liveness: HEARTBEAT_LIVENESS,
    service: service
  };

  this.reqs[rid] = req;

  var reply = new Writable({ objectMode: true });
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
    rf.apply(self, [clientId, rid, chunk, reply.opts]);
    cb(null);
  };

  reply.opts = {};

  reply.active = function() {
    return self.reqs[rid] && !req.ended && req.liveness > 0;
  };

  reply.heartbeat = function() {
    self.heartbeat();
  };

  var _end = reply.end;

  reply.end = function() {
    reply.ended = true;

    var ret = _end.apply(reply, arguments);

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

  this.emitReq(JSON.parse(data), reply);
};

Worker.prototype.dreq = function(rid) {
  delete this.reqs[rid];
};

Worker.prototype.sendReady = function() {
  this.send([MDP.WORKER, MDP.W_READY, this.service]);
};

Worker.prototype.sendDisconnect = function() {
  this.send([MDP.WORKER, MDP.W_DISCONNECT]);
};

Worker.prototype.heartbeat = function() {
  if (this._hbcheck) {
    if ((new Date()).getTime() - this._hbcheck < this.conf.heartbeat) {
      return;
    }
  }

  this.send([
    MDP.WORKER, MDP.W_HEARTBEAT, '', 
    JSON.stringify({
      concurrency: this.conf.concurrency 
    })
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

Worker.prototype.replyReject = function (clientId, rid, err) {
  this.reply(MDP.W_REPLY_REJECT, clientId, rid, 0, err);
};

Worker.prototype.replyError = function(clientId, rid, err) {
  this.reply(MDP.W_REPLY, clientId, rid, -1, err);
};

module.exports = Worker;
