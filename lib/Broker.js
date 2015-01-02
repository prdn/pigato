var util = require('util');
var debug = require('debug')('pigato:Broker');
var crypto = require('crypto');
var uuid = require('shortid');
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
  this.socket.send(msg);
};

Broker.prototype.onMsg = function(msg) {
  msg = putils.mparse(msg);

  var header = msg[1];

  if (header == MDP.CLIENT) {
    this.onClient(msg);
  } else if (header == MDP.WORKER) {
    this.onWorker(msg);
  } else {
    debug('(onMsg) Invalid message header \'' + header + '\'');
  }
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
    var clientId = msg[3];
    var rid = msg[5];

    if (rid !== worker.rid) {
      debug("B: FATAL from worker '%s' (%s), rid mismatch '%s'!='%s'", workerId, worker.service, worker.rid, rid);
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

        self.ctrl.rset(rid, req, function() {
          self.ctrl.runshift(req, function() {
            delete worker.rid;
            self.serviceWorkerWait(worker.service, workerId, true);
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
          delete worker.rid;

          self.serviceWorkerWait(worker.service, workerId, true);
          self.ctrl.rdel(req);
          
          if (opts.cache) {
            self.cache.set(
              req.hash, obj,
              { expire: opts.cache }
            );
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

  var rhash = srvName + crypto.createHash('md5')
  .update(msg[5])
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

  async.series([function(next) {
    self.ctrl.rset(rid, req, next);
  },
  function(next) {
    self.ctrl.rpush(req, next);
  },
  function(next) {
    self.dispatch(srvName, next);
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
    liveness: HEARTBEAT_LIVENESS
  };

  this.workers[workerId] = worker;
  return worker;
};

Broker.prototype.workerDelete = function(workerId, disconnect) {
  var worker = this.workers[workerId];

  if (!worker) {
    delete this.workers[workerId];
    return;
  }

  debug('workerDelete \'%s\'%s (%s)', workerId, disconnect);

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

    if (!worker) {
      self.workerDelete(workerId, true);
      return true;
    }

    worker.liveness--;

    if (worker.liveness < 0) {
      debug('B: workerPurge \'%s\'', workerId);
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

  if (!worker || !worker.service || _.indexOf(service.waiting, workerId) > -1) {
    this.workerDelete(workerId, true);
    return;
  }

  service.waiting.push(workerId);

  if (dispatch) {
    self.dispatch(srvName);
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

      async.auto({
        cache: function(next) {
          self.cache.get(req.hash, next);
        },
        handle: ['cache', function(next, res) {
          if (res.cache) {
            self.reply(MDP.W_REPLY, req, res.cache);
            self.ctrl.rdel(req);
            self.serviceWorkerWait(srvName, workerId);
            rcnt++;
            next();
            return;
          } 
          
          if (_.indexOf(req.rejects, workerId) !== -1) {
            self.ctrl.runshift(req, function() {
              self.serviceWorkerWait(srvName, workerId);
              rcnt++;
              next();
            });
            return;
          }

          req.workerId = workerId;
          worker.rid = req.rid;

          self.ctrl.rset(req.rid, req);

          var obj = [
            workerId, MDP.WORKER, MDP.W_REQUEST, req.clientId, '' 
          ].concat(req.msg.slice(4));

          self.send(obj);
          rcnt++;		
          next();
        }]
      }, next);
    });
  };

  function done() {
    if (rcnt) {
      self.dispatch(srvName);
    }
  }

  if (service.waiting.length) {
    var workerId = self.serviceWorkerUnwait(srvName);
    if (!workerId) {
      return;
    }

    var worker = self.workers[workerId];
    if (!worker) {
      return;
    }

    handle(workerId, worker, done);
  }
};

function Controller() {
  this.reqs = {};
  this.srq = {};
};

Controller.prototype.rset = function(rid, req, callback) {
  this.reqs[rid] = _.clone(req, true);

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

Controller.prototype.rpush = function(req, callback) {
  var srv = req.service;

  if (!this.srq[srv]) {
    this.srq[srv] = [];
  }
 
  var srq = this.srq[srv];
  srq.push(req.rid);
  
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

  if (callback) {
    callback();
  }
};

Controller.prototype.rdel = function(req) {
  delete this.reqs[req.rid];
  
  var srq = this.srq[req.service];
  _.pull(srq, req.rid);
};


function Cache() {
  this.m = {};

  var self = this;

  setInterval(function() {
    self.prune();   
  }, 5000);
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
