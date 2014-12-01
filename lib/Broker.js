var util = require('util');
var debug = require('debug')('pigato:Broker');
var crypto = require('crypto');
var uuid = require('shortid');
var events = require('events');
var zmq = require('zmq');
var async = require('async');
var _ = require('lodash');
var MDP = require('./mdp');

var HEARTBEAT_LIVENESS = 3;

var noop = function() {};

function Broker(endpoint, conf) {
  this.endpoint = endpoint;

  this.services = {};
  this.workers = {};

  this.conf = {
    heartbeat: 2500,
    flushConcurrency: 500
  };

  var self = this;

  Object.keys(conf || {}).every(function(k) {
    self.conf[k] = conf[k];
  });

  this.ctrl = new Controller();
  this.cache = new Cache();

  events.EventEmitter.call(this);
}
util.inherits(Broker, events.EventEmitter);

Broker.prototype.start = function(cb) {
  this.name = 'B' + uuid.generate();
  this.socket = zmq.socket('router');
  this.socket.identity = new Buffer(this.name);

  var self = this;

  this.socket.on('message', function() {
    self.onMsg.call(self, arguments);
  });

  try {
    this.socket.bindSync(this.endpoint);
  } catch(e) {
    cb(e);
    return;
  }

  if (this.conf.debug) {
    debug('B: broker started on %s', this.endpoint);
  }

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
  this.socket.send(msg);
};

Broker.prototype.onMsg = function(msg) {
  var self = this;
  var header = msg[1].toString();

  if (header == MDP.CLIENT) {
    setImmediate(function() {
      self.onClient(msg);
    });
  } else if (header == MDP.WORKER) {
    setImmediate(function() {
      self.onWorker(msg);
    });
  } else {
    if (this.conf.debug) {
      debug('(onMsg) Invalid message header \'' + header + '\'');
    }
  }
};

Broker.prototype.emitErr = function(msg) {
  this.emit.apply(self, ['error', msg]);
};

Broker.prototype.onClient = function(msg) {
  var self = this;
  var clientId = msg[0].toString();
  var type = msg[2];

  if (type == MDP.W_REQUEST) {
    var srvName = msg[3].toString();
    //debug("B: REQUEST from clientId: %s, service: %s", clientId, srvName);
    this.requestQueue(srvName, msg);
  } else if (type == MDP.W_HEARTBEAT) {
    if (msg.length === 4) {
      var rid = msg[3].toString();
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
  var workerId = msg[0].toString();
  var type = msg[2];

  var workerReady = (workerId in this.workers);
  var worker = this.workerRequire(workerId)

  if (type == MDP.W_READY) {
    var srvName = msg[3].toString();

    if (this.conf.debug) {
      debug('B: register worker: %s, service: %s', workerId, srvName, workerReady ? 'R' : '');
    }

    if (workerReady) {
      this.workerDelete(workerId, true);

    } else {
      this.serviceRequire(srvName);
      worker.service = srvName;
      this.serviceWorkerWait(srvName, workerId, true);
    }

    return;
  } 

  if (!workerReady) {
    this.workerDelete(workerId, true);
    return;
  }

  worker.liveness = HEARTBEAT_LIVENESS;

  if (type == MDP.W_REPLY || type == MDP.W_REPLY_PARTIAL || type == MDP.W_REPLY_REJECT) {
    var clientId = msg[3].toString();
    var rid = msg[5].toString();

    if (rid !== worker.rid) {
      if (this.conf.debug) {
        debug("B: FATAL from worker '%s' (%s), rid mismatch '%s'!='%s'", workerId, worker.service, worker.rid, rid);
      }
      this.workerDelete(workerId, true);
      return;
    }

    self.ctrl.rget(rid, function (err, req) {
      if (!req) {
        if (self.conf.debug) {
          debug("B: FATAL from worker '%s' (%s), req not found", workerId, worker.service, rid);
        }
        self.workerDelete(workerId, true);
        return;
      }

      if (type == MDP.W_REPLY_REJECT) {
        if (self.conf.debug) {
          debug("B: REJECT from worker '%s' (%s) for req '%s'", workerId, worker.service, rid);
        }

        req.rejects.push(workerId);
        delete req.workerId;

        self.ctrl.rset(rid, req, function() {
          self.ctrl.runshift(req, function() {
            delete worker.rid;
            self.serviceWorkerWait(worker.service, workerId, true);
          });
        });
      } else if (type == MDP.W_REPLY || type == MDP.W_REPLY_PARTIAL) {
        if (self.conf.debug) {
          debug("B: REPLY from worker '%s' (%s)", workerId, worker.service);
        }
        
        var opts = msg[8] ? msg[8].toString() : null;
        try { opts = JSON.parse(opts); } catch(e) {};
        if (!_.isObject(opts)) {
          opts = {};
        }

        var obj = [];
        for (var i = 6; i < msg.length; i++) {
          obj.push(msg[i].toString());
        }

        self.reply(type, req, obj);

        if (type == MDP.W_REPLY) {
          delete worker.rid;
          self.serviceWorkerWait(worker.service, workerId, true);
          self.ctrl.rdel(req);
          
          if (opts.cache) {
            self.cache.set(
              req.hash, obj,
              { expire: opts.cache }
            );
            self.cflush(req.hash);
          }
        }
      }
    });
  } else {
    if (type == MDP.W_DISCONNECT) {
      this.workerDelete(workerId);
    }
  }
};

Broker.prototype.cflush = function(hash) {
  var self = this;

  async.auto({
    cache: function(next) {
      self.cache.get(hash, next);
    },
    hlist: ['cache', function(next, res) {
      if (!res.cache) {
        next('ERR_CACHE_NOTFOUND');
        return;
      }
      self.ctrl.hlist(hash, next);
    }],
    run: ['hlist', function(next, res) {
      var queue = [];

      _.each(res.hlist, function(rid) {
        queue.push(function(next) {
          self.ctrl.rget(rid, function (err, req) {
            if (!req) {
              next();
              return;
            }

            self.reply(MDP.W_REPLY, req, res.cache);
            next();
          });
        });
      });

      async.parallelLimit(queue, self.conf.flushConcurrency);
    }]
  });
};

Broker.prototype.reply = function(type, req, msg) {
  var obj = [req.clientId, MDP.CLIENT, type, '', req.rid];

  for (var i = 0; i < msg.length; i++) {
    obj.push(msg[i]);
  }

  this.send(obj);
};

Broker.prototype.requestQueue = function(srvName, msg) {
  var self = this;
  var clientId = msg[0].toString();
  var rid = msg[4].toString();
  var opts = msg[6] ? msg[6].toString() : null;
  
  try { opts = JSON.parse(opts); } catch(e) {};
  if (!_.isObject(opts)) {
    opts = {};
  }

  var rhash = srvName + crypto.createHash('sha1')
  .update(msg[5].toString())
  .digest('hex');

  var req = {
    service: srvName,
    clientId: clientId,
    rid: rid,
    hash: rhash,
    timeout: opts.timeout || 60000,
    ts: (new Date()).getTime(),
    rejects: [],
    msg: msg,
    opts: opts
  };

  async.auto({
    cache: function(next) {
      self.cache.get(
        req.hash,
        function(err, data) {
          if (!err && data) {
            self.reply(MDP.W_REPLY, req, data);
            next(null, true);
            return;
          }
          next();
        }
      );
    },
    process: ['cache', function(next, res) {
      if (res.cache) {
        next();
        return;
      }

      async.series([function(next) {
        self.ctrl.rset(rid, req, next);
      },
      function(next) {
        self.ctrl.rpush(req, next);
      },
      function(next) {
        self.dispatch(srvName, next);
      }]);
    }]
  });
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
    liveness: HEARTBEAT_LIVENESS
  };

  this.workers[workerId] = worker;
  return worker;
};

Broker.prototype.workerDelete = function(workerId, disconnect) {
  var worker = this.workers[workerId];

  if (!worker) {
    return;
  }

  if (this.conf.debug) {
    debug('workerDelete \'%s\'%s (%s)', workerId, disconnect);
  }

  if (disconnect) {
    this.send([workerId, MDP.WORKER, MDP.W_DISCONNECT]);
  }

  if (worker.service) {
    var service = this.serviceRequire(worker.service);
    for (var i = 0; i < service.waiting.length; i++) {
      if (workerId !== service.waiting[i]) {
        continue;
      }

      service.waiting.splice(i, 1);
      break;
    }
  }

  delete this.workers[workerId];
};

Broker.prototype.workersCheck = function() {
  var self = this;

  Object.keys(this.workers).every(function(workerId) {
    var worker = self.workers[workerId];
    worker.liveness--;

    if (worker.liveness < 0) {
      if (self.conf.debug) {
        debug('B: workerPurge \'%s\'', workerId);
      }
      self.workerDelete(workerId, true);
      return true;
    }

    self.send([workerId, MDP.WORKER, MDP.W_HEARTBEAT]);

    return true;
  });
};

Broker.prototype.serviceRequire = function(srvName) {
  if (srvName in this.services) {
    return this.services[srvName];
  }

  var service = {
    name: srvName,
    waiting: []
  };

  this.services[srvName] = service;
  return service;
};

Broker.prototype.serviceWorkerWait = function(srvName, workerId, dispatch) {
  var self = this;
  var service = this.serviceRequire(srvName);
  var worker = this.workers[workerId];

  if (!worker || !worker.service || service.waiting.indexOf(workerId) > -1) {
    this.workerDelete(workerId, true);
    return;
  }

  service.waiting.push(workerId);

  if (dispatch) {
    setImmediate(function() {
      self.dispatch(srvName);
    });
  }
};

Broker.prototype.serviceWorkerUnwait = function(srvName) {
  var service = this.serviceRequire(srvName);
  if (service.waiting.length) {
    return service.waiting.shift();
  }
  return null;
};

Broker.prototype.dispatch = function(srvName) {
  var self = this;
  var service = this.serviceRequire(srvName);
  var rcnt = 0;

  var handle = function(workerId, worker, next) {
    self.ctrl.rpop(srvName, function(err, req) {
      if (err || !self.requestValidate(req)) {
        if (req) {
          self.ctrl.rdel(req);
          rcnt++;
        }
        self.serviceWorkerWait(srvName, workerId);
        next();
        return;
      }

      if (req.rejects.indexOf(workerId) !== -1) {
        self.ctrl.runshift(req, next);
        return;
      }

      req.workerId = workerId;
      worker.rid = req.rid;

      self.ctrl.rset(req.rid, req);

      var obj = [
        workerId, MDP.WORKER, MDP.W_REQUEST, req.clientId, '' 
      ];

      for (var i = 4; i < req.msg.length; i++) {
        obj.push(req.msg[i]);
      }

      self.send(obj);
      rcnt++;		
      next();
    });
  };

  var swcnt = service.waiting.length;
  var queue = [];

  for (var service_index = 0; service_index < swcnt; service_index++) {
    queue.push(function(next) {
      var workerId = self.serviceWorkerUnwait(srvName);
      if (!workerId) {
        next();
        return;
      }

      var worker = self.workers[workerId];
      if (!worker) {
        next();
        return;
      }

      handle(workerId, worker, next);
    });
  }

  async.series(queue, function() {
    if (rcnt) {
      setImmediate(function() {
        self.dispatch(srvName);
      });
    }
  });
};

function Controller() {
  this.reqs = {};
  this.srq = {};
  this.hm = {};
};

Controller.prototype.rset = function(rid, req, callback) {
  this.reqs[rid] = req;

  if (callback) {
    callback();
  }
};

Controller.prototype.rget = function(rid, callback) {
  var self = this;

  if (callback) {
    setImmediate(function() {
      callback(null, self.reqs[rid]);
    });
  }
};

Controller.prototype.rpush = function(req, callback) {
  var srv = req.service;

  if (!this.srq[srv]) {
    this.srq[srv] = [];
  }
 
  var srq = this.srq[srv];
  srq.push(req.rid);
  
  if (!this.hm[req.hash]) {
    this.hm[req.hash] = [];
  }
 
  var hm = this.hm[req.hash];
  hm.push(req.rid);
  
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

    var hm = self.hm[req.hash];
    var rix = hm.indexOf(req.rid);
    if (rix > -1) {
      hm.splice(rix, 1);
    }
    
    if (!hm.length) {
      delete self.hm[req.hash];
    }

    callback(null, req);
  });
};

// adds a request to the front of a queue
// used for retry logic
Controller.prototype.runshift = function(req, callback) {
  var srv = req.service;
  
  if (!this.srq[srv]) {
    this.srq[srv] = [];
  }

  var srq = this.srq[srv];
  srq.unshift(req.rid);

  if (!this.hm[req.hash]) {
    this.hm[req.hash] = [];
  }
 
  var hm = this.hm[req.hash];
  hm.unshift(req.rid);

  if (callback) {
    callback();
  }
};

Controller.prototype.rdel = function(req) {
  delete this.reqs[req.rid];
  
  var srq = this.srq[req.service];

  var srix = srq.indexOf(req.rid);
  if (srix > -1) {
    srq.splice(srix, 1);
  }

  var hm = this.hm[req.hash];
  if (!hm || !hm.length) {
    delete this.hm[req.hash];
  }
};

Controller.prototype.hlist = function(hash, callback) {
  var self = this;

  var hm = this.hm[hash];
  if (!hm) {
    callback(null, []);
    return;
  }
 
  setImmediate(function() {
    callback(null, _.clone(hm));
  });
};

function Cache() {
  this.m = {};

  var self = this;

  setInterval(function() {
    self.prune();   
  }, 10000);
};

Cache.prototype.prune = function() {
  var self = this;

  Object.keys(this.m).every(function(hash) {
    var entry = self.m[hash];
    if (entry.expire > -1) {
      if (entry.expire < (new Date()).getTime()) {
        delete self.m[hash];
      }
    }

    return true;
  });
};

Cache.prototype.get = function(hash, callback) {
  var self = this;

  setImmediate(function() {
    var entry = self.m[hash];
    callback(null, entry ? entry.data : null);
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
