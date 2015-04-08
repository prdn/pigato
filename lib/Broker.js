'use strict';

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

var BaseController = require('./BrokerController');

var HEARTBEAT_LIVENESS = 3;

var noop = function() {};

function Broker(endpoint, conf) {
  this.endpoint = endpoint;

  this.services = {};
  this.workers = {};
  this.rmap = {};

  this.conf = {
    heartbeat: 2500,
    dmode: 'load'
  };

  _.extend(this.conf, conf);

  if (this.conf.ctrl) {
    this.ctrl = this.conf.ctrl;
  } else {
    this.ctrl = new BaseController();
  }

  this.cache = {};

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
    setImmediate(function() {
      self.onMsg.call(self, args);
    });
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

  this.ctrl.rgetall(function(err, reqs) {
    _.each(reqs, function(req) {
      self.requestQueue(req);
    });

    if (cb) {
      cb();
    }
  });

  setInterval(function() {
    _.each(self.cache, function(v, k) {
      if (v.expire < (new Date()).getTime()) {
        delete self.cache[k]; 
      }
    });

    var supd = false;

    _.each(self.services, function(service, srvName) {
      if (!service.workers.length) {
        supd = true;
        delete self.services;
      }
    });

    if (supd) {
      self.servicesUpdate();
    }
  }, 60000);
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

Broker.prototype.onMsg = function(_msg) {
  var self = this;
  var msg = putils.mparse(_msg);

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
    var srvName = msg[3] || 'UNK';
    var rid = msg[4];
    debug("B: REQUEST from clientId: %s, service: %s", clientId, srvName);

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
      attempts: 0,
      rid: rid,
      hash: rhash,
      timeout: opts.timeout || 60000,
      ts: (new Date()).getTime(),
      rejects: [],
      msg: msg,
      opts: opts
    };

    this.requestQueue(req);
  } else if (type == MDP.W_HEARTBEAT) {
    if (msg.length === 4) {
      var rid = msg[3];
      var req = this.rmap[rid];
      if (req && req.workerId) {
        self.send([req.workerId, MDP.WORKER, MDP.W_HEARTBEAT, clientId, '', rid]);
      }
    }
  }
};

Broker.prototype.onWorker = function(msg) {
  var self = this;
  var workerId = msg[0];
  var type = msg[2];

  var wready = (workerId in this.workers);
  var worker = this.workerRequire(workerId)

  if (type == MDP.W_READY) {
    var srvName = msg[3];

    debug('B: register worker: %s, service: %s', workerId, srvName, wready ? 'R' : '');

    if (!srvName) {
      this.workerDelete(workerId, true);

    } else if (!wready) {
      this.serviceRequire(srvName);
      this.serviceWorkerAdd(srvName, workerId);
    }
    return;
  } 

  if (!wready) {
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

    var req = this.rmap[rid];
    if (!req) {
      debug("B: FATAL from worker '%s' (%s), req not found", workerId, worker.service, rid);
      this.workerDelete(workerId, true);
      return;
    }
  
    var service = this.serviceRequire(req.service);

    if (type == MDP.W_REPLY_REJECT) {
      debug("B: REJECT from worker '%s' (%s) for req '%s'", workerId, worker.service, rid);

      req.rejects.push(workerId);
      delete req.workerId;
      _.pull(worker.rids, rid);

      service.q.push(req);
      this.dispatch(worker.service, 'rand');

    } else if (type == MDP.W_REPLY || type == MDP.W_REPLY_PARTIAL) {
      debug("B: REPLY from worker '%s' (%s)", workerId, worker.service);

      var opts = msg[8];
      try { opts = JSON.parse(opts); } catch(e) {};
      if (!_.isObject(opts)) {
        opts = {};
      }

      var obj = msg.slice(6);

      this.reply(type, req, obj);

      if (type == MDP.W_REPLY) {
        _.pull(worker.rids, rid);

        delete this.rmap[req.rid];
        if (req.opts.persist) {
          this.ctrl.rdel(req);
        }

        if (self.conf.cache && opts.cache) {
          this.cache[req.hash] = {
            data: obj,
            expire: new Date().getTime() + opts.cache
          };
        }

        this.dispatch(worker.service);
      }
    }

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

Broker.prototype.requestQueue = function(req) {
  var service = this.serviceRequire(req.service);
  service.q.push(req);
 
  if (req.opts.persist) {
    this.ctrl.rset(req);
  }

  this.dispatch(req.service);
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
  if (this.workers[workerId]) {
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

  var service = null;
  if (worker.service) {
    service = this.serviceRequire(worker.service);
    _.pull(service.workers, workerId);
  }

  delete this.workers[workerId];

  for (var r = 0; r < worker.rids.length; r++) {
    var rid = worker.rids[r];
    var req = this.rmap[rid];
    if (!req) {
      return;
    }

    delete this.rmap[rid];
    delete req.workerId;

    var crd = true;

    if (worker.service) {
      if (req.opts.retry) {
        service.q.push(req);
        crd = false;
      }
    }

    if (crd && req.opts.persist) {
      this.ctrl.rdel(req);
    }
  }

  if (worker.service) {
    this.dispatch(worker.service);
  }
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

Broker.prototype.workerAvailable = function(workerId) {
  if (!workerId) {
    return false;
  }

  var worker = this.workers[workerId];
  if (!worker) {
    return false;
  }
  
  if (worker.opts.concurrency === -1) {
    return true;
  }

  if (worker.rids.length < worker.opts.concurrency) {
    return true;
  }

  return false;
};

Broker.prototype.serviceRequire = function(srvName) {
  if (this.services[srvName]) {
    return this.services[srvName];
  }

  var service = {
    name: srvName,
    workers: [],
    q: [],
    aux: []
  };
     
  this.services[srvName] = service;
  this.servicesUpdate();
  return service;
};

Broker.prototype.servicesUpdate = function() {
  var self = this;

  _.each(this.services, function(service, srvName) {
    service.aux = [srvName];
  });
    
  _.each(this.services, function(service, srvName) {
    var isWC = srvName[srvName.length - 1] === '*';
  
    _.each(self.services, function(aService, aSrvName) {
      if (aSrvName === srvName) {
        return;
      }

      var rel = false;
      if (isWC) {
        if(aSrvName.indexOf(srvName.slice(0, -1)) === 0) {
          rel = true;
        }
      } else {
        var aIsWC = aSrvName[aSrvName.length - 1] === '*';

        if (aIsWC && srvName.indexOf(aSrvName.slice(0, -1)) === 0) {
          rel = true;
        }
      }

      if (rel) {
        aService.aux.push(srvName);
        service.aux.push(aSrvName);
      }
    });
  });
  
  _.each(this.services, function(service, srvName) {
    service.aux = _.unique(service.aux);
  });
};

Broker.prototype.serviceWorkerAdd = function(srvName, workerId) {
  var service = this.serviceRequire(srvName);
  var worker = this.workerRequire(workerId);

  if (!worker) {
    this.workerDelete(workerId, true);
    return;
  }

  if (_.indexOf(service.workers, workerId) === -1) {
    worker.service = srvName;
    service.workers.push(workerId);
  }

  this.dispatch(srvName);
};

function _rhandle(srvName, workerIds) {
  var self = this;

  var service = this.serviceRequire(srvName);
  
  var req = service.q.shift();
  if (!req) {
    return 0;
  }
 
  var vld = this.requestValidate(req);
  
  if (!vld || req.attempts >= this.conf.rattempts) {
    delete this.rmap[req.rid];
    if (req.opts.persist) {
      this.ctrl.rdel(req);
    }

    return service.q.length;
  }
  
  if (this.conf.cache && !req.opts.nocache) {
    var chit = this.cache[req.hash];
    
    if (chit) {
      if (chit.expire < (new Date()).getTime()) {
        delete this.cache[req.hash]; 

      } else {
        this.reply(MDP.W_REPLY, req, chit.data);
        
        delete this.rmap[req.rid];
        if (req.opts.persist) {
          this.ctrl.rdel(req);
        }
        
        return service.q.length;
      }
    }
  }
 
  var workerId = null;

  if (req.opts.workerId) {
    if (self.workerAvailable(req.opts.workerId)) {
      workerId = req.opts.workerId;
    }
  } else {
    for (var wi = 0; wi < workerIds.length; wi++) {
      var _workerId = workerIds[wi];
      if (self.workerAvailable(_workerId)) {
        if (_.indexOf(req.rejects, _workerId) === -1) {
          workerId = _workerId;
          break;
        }
      }
    }
  }

  if (!workerId) {
    service.q.unshift(req);
    return 0;
  }

  req.attempts++;
  
  var worker = this.workerRequire(workerId);
  
  this.rmap[req.rid] = req;

  req.workerId = worker.workerId;
  worker.rids.push(req.rid);

  if (req.opts.persist) {
    this.ctrl.rset(req);
  }

  var obj = [
    worker.workerId, MDP.WORKER, MDP.W_REQUEST, 
    req.clientId, req.service, ''
  ].concat(req.msg.slice(4));

  this.send(obj);
  
  return service.q.length;
};

Broker.prototype.dispatch = function(srvName) {
  var self = this;
  var service = this.serviceRequire(srvName);

  var workerIds = [];
  for (var si = 0; si < service.aux.length; si++) {
    var aService = this.serviceRequire(service.aux[si]);
    workerIds = workerIds.concat(aService.workers);
  }

  if (this.conf.dmode === 'load') {
    workerIds.sort(function(a, b) {
      return self.workers[a].rids.length <= self.workers[b].rids.length ? -1 : 1;
    });
  }

  while (1) {
    var ret = _rhandle.call(self, srvName, workerIds);
    if (!ret) {
      break;
    }
  }
};

module.exports = Broker;
