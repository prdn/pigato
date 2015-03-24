var zmq = require('zmq');
var Readable = require('readable-stream').Readable;
var debug = require('debug')('pigato:Client');
var uuid = require('node-uuid');
var util = require('util');
var events = require('events');
var _ = require('lodash');
var MDP = require('./mdp');
var putils = require('./utils');

var HEARTBEAT_LIVENESS = 3;

function Client(brokers, conf) {
  if (_.isString(brokers)) {
    brokers = [brokers];
  }
  this.brokers = brokers;

  this.conf = {
    autostart: false,
    heartbeat: 2500,
    timeout: 60000,
    retry: 0,
    name: 'C' + uuid.v4()
  };
  
  this.reqs = {};

  _.extend(this.conf, conf);

  events.EventEmitter.call(this);

  if (this.conf.autostart) {
    this.start();
  } 
};
util.inherits(Client, events.EventEmitter);

Client.prototype.start = function() {
  var self = this;

  this.socket = zmq.socket('dealer');
  this.socket.identity = new Buffer(this.conf.name);

  this.socket.on('message', function() {
    self.onMsg.call(self, putils.args(arguments));
  });

  this.socket.on('error', function(err) {
    self.emitErr(err);
  });

  _.each(this.brokers, function(b) {
    self.socket.connect(b);
  });

  debug('Client connected to %s', this.broker);

  this.hbTimer = setInterval(function() {
    self.heartbeat();
    _.each(self.reqs, function(req, rid) {
      if (req.timeout > -1 && ((new Date()).getTime() > req.lts + req.timeout)) {
        self.onMsg([MDP.CLIENT, MDP.W_REPLY, '', new Buffer(rid), new Buffer('-1'), new Buffer(JSON.stringify('C_TIMEOUT'))]);
      }
    });
  }, this.conf.heartbeat);
};

Client.prototype.stop = function() {
  clearInterval(this.hbTimer);
  if (this.socket) {
    if(this.socket._zmq.state != zmq.STATE_CLOSED) {
      this.socket.close();
    }
    delete this.socket;
  }
};

Client.prototype.send = function(msg) {
  if (!this.socket) {
    return;
  }
  this.socket.send(msg);
};

Client.prototype.onMsg = function(msg) {
  msg = putils.mparse(msg);

  var header = msg[0];
  var type = msg[1];

  if (header != MDP.CLIENT) {
    this.emitErr('ERR_MSG_HEADER');
    return;
  }

  if (msg.length < 3) {
    return;
  }

  var rid = msg[3];
  var req = this.reqs[rid];
  if (!req) {
    return;
  }

  var err = +msg[4] || 0;
  var data = msg[5] || null;

  if (data) {
    data = JSON.parse(data);
  }

  if (err === -1) {
    err = data;
    data = null;
  }

  if (type == MDP.W_REPLY || type == MDP.W_REPLY_PARTIAL) {
    req.lts = new Date().getTime();

    if (type == MDP.W_REPLY) {
      req._finalMsg = [err, data];
      req.ended = true;
      delete this.reqs[rid];
    }

    if (err) {
      req.stream.emit('error', err);
    }

    req.stream.push(data);

    if (type == MDP.W_REPLY) {
      req.stream.push(null);
    }
  } else {
    this.emitErr('ERR_MSG_TYPE');
  }
};

Client.prototype.emitErr = function(msg) {
  this.emit.apply(this, ['error', msg]);
};

function noop() {}

function _request(serviceName, data, opts) {
  var self = this;
  var rid = uuid.v4();

  if (typeof opts !== 'object') {
    opts = {};
  }	

  opts.timeout = opts.timeout || this.conf.timeout;
  opts.retry = opts.retry || this.conf.retry;

  var req = this.reqs[rid] = {
    rid: rid,
    timeout: opts.timeout,
    ts: new Date().getTime(),
    opts: opts,
    heartbeat: function() {
      self.heartbeat(rid);
    },
    _finalMsg: null, 
    ended: false
  };

  req.lts = req.ts;

  var stream = new Readable({ objectMode: true });

  stream._read = noop;
  stream.heartbeat = req.heartbeat;

  req.stream = stream;

  debug('C: send request', serviceName, rid);

  this.send([
    MDP.CLIENT, MDP.W_REQUEST, serviceName, rid, 
    JSON.stringify(data), JSON.stringify(opts)
  ]);

  return req;
};

Client.prototype.requestStream = function(serviceName, data, opts) {
  return this.request(serviceName, data, opts);
};

Client.prototype.request = function(serviceName, data, partialCb, finalCb, opts) {
  var mode = 'stream';
  var serviceName = arguments[0];
  var data = arguments[1];
  var opts, partialCb, finalCb;

  if (arguments.length >= 4) {
    mode = 'callback';
    partialCb = arguments[2];
    finalCb = arguments[3];
    opts = arguments[4];
  } else {
    opts = arguments[2];
  }

  var req = _request.call(this, serviceName, data, opts);

  if (mode === 'callback') {
    req.stream.on('data', function(data) {
      if (req.ended) {
        return;
      }

      if (partialCb) {
        partialCb(null, data);
      }
    });

    req.stream.on('end', function() {
      var msg = req._finalMsg;

      if (finalCb) {
        finalCb(msg[0], msg[1]);
      }
    });

    req.stream.on('error', noop);

  } else {
    return req.stream;
  }
};

Client.prototype.heartbeat = function(rid) {
  var msg = [MDP.CLIENT, MDP.W_HEARTBEAT];
  if (rid) {
    msg.push(rid);
  }
  this.send(msg);
};

module.exports = Client;
