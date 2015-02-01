var util = require('util');
var debug = require('debug')('pigato:Broker');
var crypto = require('crypto');
var uuid = require('node-uuid');
var events = require('events');
var zmq = require('zmq');
var async = require('async');
var _ = require('lodash');
var MDP = require('./mdp');
var putils = require('./utils');

var HEARTBEAT_LIVENESS = 3;

var noop = function() {};

function Broker(endpoint, conf) {
  this.endpoint = endpoint;

  this.services = {};
  this.workers = {};

  this.conf = {
    heartbeat: 2500
  };

  _.extend(this.conf, conf);

  this.ctrl = new Controller();
  this.cache = new Cache();

  events.EventEmitter.call(this);
}
util.inherits(Broker, events.EventEmitter);

Broker.prototype.start = function(cb) {
  this.name = 'B' + uuid.v4();
  this.socket = zmq.socket('router');
  this.socket.identity = new Buffer(this.name);

  var self = this;

  this.socket.on('message', function() {
    var args = putils.args(arguments);
    self.onMsg.call(self, args);
  });

  try {
    this.socket.bindSync(this.endpoint);
  } catch(e) {
    cb(e);
    return;
  }

  debug('B: broker started on %s', this.endpoint);

  this.hbTimer = setInterval(function() {
    self.workersCheck();
  }, this.conf.heartbeat);

  cb();
};

Broker.prototype.stop = function() {
  clearInterval(this.hbTimer);
  if (this.socket) {
    this.socket.close();
    delete this['socket'];
  }
};

Broker.prototype.send = function(msg) {
  if(!this.socket) {
    return;
  }

  this.socket.send(msg);    
};

Broker.prototype.onMsg = function(msg) {
  var self = this;

  msg = putils.mparse(msg);

  var header = msg[1];

  if (header == MDP.CLIENT) {
    this.onClient(msg);
  } else if (header == MDP.WORKER) {
    this.onWorker(msg);
  } else {
    debug('(onMsg) Invalid message header \'' + header + '\'');
  }
  
  this.workersCheck();
};

Broker.prototype.emitErr = function(msg) {
  this.emit.apply(self, ['error', msg]);
};

Broker.prototype.onClient = function(msg) {
  var self = this;

  var clientId = msg[0];
  var type = msg[2];

  if (type == MDP.W_REQUEST) {
    var srvName = msg[3];
    debug("B: REQUEST from clientId: %s, service: %s", clientId, srvName);
    this.requestQueue(srvName, msg);
  } else if (type == MDP.W_HEARTBEAT) {
    if (msg.length === 4) {
      var rid = msg[3];
      this.ctrl.rget(rid, function(err, req) {
        if (req && req.workerId) {
          self.send([req.workerId, MDP.WORKER, MDP.W_HEARTBEAT, clientId, '', rid]);
        }
      });
    }
  }
};

Broker.prototype.onWorker = function(msg) {
  var self = this;
  var workerId = msg[0];
  var type = msg[2];

  var workerReady = (workerId in this.workers);
  var worker = this.workerRequire(workerId)

  if (type == MDP.W_READY) {
    var srvName = msg[3];

    debug('B: register worker: %s, service: %s', workerId, srvName, workerReady ? 'R' : '');

    if (workerReady) {
      this.workerDelete(workerId, true);

    } else {
      this.serviceRequire(srvName);
      worker.service = srvName;
      this.serviceWorkerAdd(srvName, workerId);
    }

    return;
  } 

  if (!workerReady) {
    this.workerDelete(workerId, true);
    return;
  }

  worker.liveness = HEARTBEAT_LIVENESS;

  if (type == MDP.W_REPLY || type == MDP.W_REPLY_PARTIAL || type == MDP.W_REPLY_REJECT) {
    var clientId = msg[3];
    var rid = msg[5];

    if (_.indexOf(worker.rids, rid) === -1) {
      debug("B: FATAL from worker '%s' (%s), rid not found mismatch '%s'", workerId, worker.service, rid);
      this.workerDelete(workerId, true);
      return;
    }

    self.ctrl.rget(rid, function (err, req) {
      if (!req) {
        debug("B: FATAL from worker '%s' (%s), req not found", workerId, worker.service, rid);
        self.workerDelete(workerId, true);
        return;
      }

      if (type == MDP.W_REPLY_REJECT) {
        debug("B: REJECT from worker '%s' (%s) for req '%s'", workerId, worker.service, rid);

        req.rejects.push(workerId);
        delete req.workerId;

        self.ctrl.rset(req, function() {
          self.ctrl.rpush(req, 'l', function() {
            _.pull(worker.rids, rid);
            self.dispatch(worker.service);
          });
        });

      } else if (type == MDP.W_REPLY || type == MDP.W_REPLY_PARTIAL) {
        debug("B: REPLY from worker '%s' (%s)", workerId, worker.service);

        var opts = msg[8];
        try { opts = JSON.parse(opts); } catch(e) {};
        if (!_.isObject(opts)) {
          opts = {};
        }

        var obj = msg.slice(6);

        self.reply(type, req, obj);

        if (type == MDP.W_REPLY) {
          _.pull(worker.rids, rid);

          self.ctrl.rdel(req);
          
          if (opts.cache) {
            self.cache.set(
              req.hash, obj,
              { expire: opts.cache }
            );
          }
          
          self.dispatch(worker.service);
        }
      }
    });

  } else {
    if (type == MDP.W_HEARTBEAT) {
      var opts = msg[4];
      try { opts = JSON.parse(opts); } catch(e) {};
      _.extend(worker.opts, opts);

    } else if (type == MDP.W_DISCONNECT) {
      this.workerDelete(workerId);
    }
  }
};

Broker.prototype.reply = function(type, req, msg) {
  this.send([req.clientId, MDP.CLIENT, type, '', req.rid].concat(msg));
};

Broker.prototype.requestQueue = function(srvName, msg) {
  var self = this;

  var clientId = msg[0];
  var rid = msg[4];
  var opts = msg[6];
  
  try { opts = JSON.parse(opts); } catch(e) {};
  if (!_.isObject(opts)) {
    opts = {};
  }

  var rhash = null;
  if (this.conf.cache) {
    rhash = srvName + crypto.createHash('md5')
    .update(msg[5])
    .digest('hex');
  }

  var req = {
    service: srvName,
    clientId: clientId,
    rid: rid,
    hash: rhash,
    timeout: opts.timeout || 60000,
    retry: opts.retry || 0,
    ts: (new Date()).getTime(),
    rejects: [],
    msg: msg,
    opts: opts
  };

  async.series([function(next) {
    self.ctrl.rset(req, next);
  },
  function(next) {
    self.ctrl.rpush(req, 'r', next);
  },
  function(next) {
    self.dispatch(srvName);
    next();
  }]);
};

Broker.prototype.requestValidate = function(req) {
  if (!req) {
    return false;
  }

  if (req.timeout > -1 && ((new Date()).getTime() > req.ts + req.timeout)) {
    return false;
  }

  return true;
};	

Broker.prototype.workerRequire = function(workerId) {
  if (workerId in this.workers) {
    return this.workers[workerId];
  }

  var worker = {
    workerId: workerId,
    liveness: HEARTBEAT_LIVENESS,
    rids: [],
    opts: {
      concurrency: 100
    }
  };

  this.workers[workerId] = worker;
  return worker;
};

Broker.prototype.workerDelete = function(workerId, disconnect) {
  var self = this;

  var worker = this.workers[workerId];

  if (!worker) {
    delete this.workers[workerId];
    return;
  }

  debug('workerDelete \'%s\' (%s)', workerId, disconnect);

  if (disconnect) {
    this.send([workerId, MDP.WORKER, MDP.W_DISCONNECT]);
  }

  if (worker.service) {
    var service = this.serviceRequire(worker.service);
    _.pull(service.workers, workerId);
  }

  delete this.workers[workerId];
  
  var queue = [];

  _.each(worker.rids, function(rid) {
    queue.push(function(next) {
      self.ctrl.rget(rid, function (err, req) {
        if (req) {
          delete req.workerId;
          self.ctrl.rset(req, function() {
            if (req.retry) {
              self.ctrl.rpush(req, 'l', function() {
                next();
              });
            } else {
              self.ctrl.rdel(req);
              next();
            }
          });
        } else {
          next();
        }
      });
    });
  });

  async.series(queue, function() {
    self.dispatch(worker.service); 
  });
};

Broker.prototype.workersCheck = function() {
  var self = this;

  if (this._wcheck) {
    if ((new Date()).getTime() - this._wcheck < this.conf.heartbeat) {
      return;
    }
  }

  this._wcheck = (new Date()).getTime();

  _.each(this.workers, function(worker, workerId) {
    if (!worker) {
      self.workerDelete(workerId, true);
      return;
    }

    worker.liveness--;

    if (worker.liveness < 0) {
      debug('B: workerPurge \'%s\'', workerId);
      self.workerDelete(workerId, true);
      return;
    }

    self.send([workerId, MDP.WORKER, MDP.W_HEARTBEAT]);
  });
};

Broker.prototype.serviceRequire = function(srvName) {
  if (srvName in this.services) {
    return this.services[srvName];
  }

  var service = {
    name: srvName,
    workers: []
  };

  this.services[srvName] = service;
  return service;
};

Broker.prototype.serviceWorkerAdd = function(srvName, workerId) {
  var self = this;
  var service = this.serviceRequire(srvName);
  var worker = this.workers[workerId];

  if (!worker || !worker.service || _.indexOf(service.workers, workerId) > -1) {
    this.workerDelete(workerId, true);
    return;
  }

  service.workers.push(workerId);

  self.dispatch(srvName);
};

function wsel(service, mode) {
  if (mode === 'rand') {
    var workerId = service.workers[_.random(0, service.workers.length - 1)];
    var worker = this.workers[workerId];
    if (!worker) {
      return null;
    }

    if (worker.opts.concurrency > -1 && worker.rids.length > worker.opts.concurrency) {
      return null;
    }

    return workerId;
    
  } else if (mode === 'load') {

  }

  return null;
}

Broker.prototype.serviceWorkerSelect = function(srvName) {
  var service = this.serviceRequire(srvName);
  return wsel.call(this, service, 'rand');
};

function _rproc(worker, req, callback) {
  if (_.indexOf(req.rejects, worker.workerId) !== -1) {
    this.ctrl.rpush(req, 'l', function() {
      callback(null, 1);
    });
    return;
  }

  req.workerId = worker.workerId;
  worker.rids.push(req.rid);

  this.ctrl.rset(req);

  var obj = [
    worker.workerId, MDP.WORKER, MDP.W_REQUEST, req.clientId, '' 
  ].concat(req.msg.slice(4));

  this.send(obj);
  callback();
};

function _rhandle(worker, callback) {
  var self = this;

  this.ctrl.rpop(worker.service, function(err, req, len) {
    var rcnt = 0;

    if (req) {
      if (_.isFinite(len)) {
        if (len) {
          rcnt++;
        }
      } else {
        rcnt++;
      }
    }

    if (err || !self.requestValidate(req)) {
      if (req) {
        self.ctrl.rdel(req);
      }
      callback(null, rcnt);
      return;
    }

    if (self.conf.cache) {
      async.auto({
        cache: function(next) {
          self.cache.get(req.hash, next);
        },
        handle: ['cache', function(next, res) {
          if (res.cache) {
            self.reply(MDP.W_REPLY, req, res.cache);
            self.ctrl.rdel(req);
            next();
            return;
          } 

          _rproc.call(self, worker, req, next);
        }]
      }, callback);
    } else {
      _rproc.call(self, worker, req, callback);
    }
  });
};

Broker.prototype.dispatch = function(srvName) {
  var self = this;

  var service = this.serviceRequire(srvName);

  function done(err, rcnt) {
    if (rcnt) {
      setImmediate(function() {
        self.dispatch(srvName);
      });
    }
  }

  if (service.workers.length) {
    var workerId = self.serviceWorkerSelect(srvName);
    if (!workerId) {
      done();
      return;
    }

    _rhandle.call(this, self.workers[workerId], done);
  } else {
    done();
  }
};

function Controller() {
  this.reqs = {};
  this.srq = {};
};

Controller.prototype.rset = function(req, callback) {
  this.reqs[req.rid] = _.clone(req, true);

  if (callback) {
    callback();
  }
};

Controller.prototype.rget = function(rid, callback) {
  var self = this;

  setImmediate(function() {
    callback(null, _.clone(self.reqs[rid], true));
  });
};

Controller.prototype.rpush = function(req, dir, callback) {
  var srv = req.service;

  if (!this.srq[srv]) {
    this.srq[srv] = [];
  }
 
  var srq = this.srq[srv];
  if (dir === 'l') {
    srq.unshift(req.rid);
  } else {
    srq.push(req.rid);
  }
  
  if (callback) {
    callback();
  }
};

Controller.prototype.rpop = function(srv, callback) {
  var self = this;

  var srq = this.srq[srv];

  if (!srq) {
    callback();
    return;
  }

  this.rget(srq.shift(), function(err, req) {
    if (err) {
      callback(err);
      return;
    }

    if (!req) {
      callback('ERR_REQ_NOTFOUND');
      return;
    }

    callback(null, req, srq.length);
  });
};

Controller.prototype.rdel = function(req) {
  delete this.reqs[req.rid];

  if (!req.workerId) {
    var srq = this.srq[req.service];
    _.pull(srq, req.rid);
  }
};


function Cache() {
  this.m = {};

  var self = this;

  setInterval(function() {
    self.prune();   
  }, 10000);
};

Cache.prototype.valid = function(entry) {
  if (entry.expire > -1) {
    if (entry.expire < (new Date()).getTime()) {
      return false;
    }
  }
  return true;
};

Cache.prototype.prune = function() {
  var self = this;

  _.each(this.m, function(entry, hash) {
    if (!self.valid(entry)) {
      delete self.m[hash];
    }
  });
};

Cache.prototype.get = function(hash, callback) {
  var self = this;

  setImmediate(function() {
    var entry = self.m[hash];
    if (entry && !self.valid(entry)) {
      delete self.m[hash];
      entry = null;
    }

    callback(null, entry ? _.clone(entry.data, true) : null);
  });
};

Cache.prototype.set = function(hash, data, opts, callback) {
  this.m[hash] = {
    data: data,
    expire: opts.expire ? (new Date()).getTime() + opts.expire : -1
  };

  if (callback) {
    callback();
  }
};

module.exports = Broker;
