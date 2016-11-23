'use strict';

require('es6-shim');

var util = require('util');
var debug = require('debug')('pigato:Broker');
var uuid = require('uuid');
var events = require('events');
var zmq = require('zmq');
var async = require('async');
var _ = require('lodash');
var semver = require('semver');
var MDP = require('./mdp');
var putils = require('./utils');

var BaseController = require('./BrokerController');

var HEARTBEAT_LIVENESS = 3;

function Broker(endpoint, conf) {
  this.services = {};
  this.workers = new Map();
  this.rmap = new Map();

  this.conf = {
    heartbeat: 2500,
    dmode: 'load',
    name: 'B' + uuid.v4(),
    intch: 'tcp://127.0.0.1:55550'
  };

  _.extend(this.conf, conf);

  this.endpoint = endpoint;

  if (this.conf.ctrl) {
    this.ctrl = this.conf.ctrl;
  } else {
    this.ctrl = new BaseController();
  }

  events.EventEmitter.call(this);
}
util.inherits(Broker, events.EventEmitter);

Broker.prototype.start = function() {
  this.socket = zmq.socket('router');
  this.socket.identity = this.conf.name;
  this.socket.setsockopt('linger', 1);

  this.pub = zmq.socket('pub');
  this.pub.identity = this.conf.name + '/pub';

  var self = this;

  this.socket.on('message', function() {
    var args = putils.args(arguments);
    self.onMsg.call(self, args);
  });

  this.hbTimer = setInterval(function() {
    self.workersCheck();
  }, this.conf.heartbeat);

  this.srvTimer = setInterval(function() {
    var supd = false;

    _.each(self.services, function(service, srvName) {
      if (!service.workers.size) {
        supd = true;
        delete self.services[srvName];
      }
    });

    if (supd) {
      self.servicesUpdate();
    }

    self.rmap.forEach(function(req) {
      var vld = self.requestValidate(req);
      if (!vld) {
        self.requestDelete(req);
      }
    });
  }, 60000);

  var queue = [];

  queue.push(function(next) {
    self.socket.bind(self.endpoint, function() {
      next();
    });
  });

  queue.push(function(next) {
    self.pub.bind(self.conf.intch, function() {
      next();
    });
  });

  queue.push(function(next) {
    self.ctrl.rgetall(function(err, reqs) {
      _.each(reqs, function(req) {
        self.requestProcess(req);
      });
      next();
    });
  });

  async.series(queue, function() {
    debug('B: broker started on %s', self.endpoint);
    setImmediate(self.onStart.bind(self));
  });
};

Broker.prototype.stop = function() {
  var self = this;

  clearInterval(this.hbTimer);
  clearInterval(this.srvTimer);

  var queue = [];

  queue.push(function(next) {
    setTimeout(function() {
      next();
    }, 100);
  });

  if (self.socket) {
    queue.push(function(next) {
      self.socket.unbind(self.endpoint, function() {
        self.socket.close();
        delete self.socket;
        next();
      });
    });
  }

  if (self.pub) {
    queue.push(function(next) {
      self.pub.unbind(self.conf.intch, function() {
        self.pub.close();
        delete self.pub;
        next();
      });
    });
  }

  async.series(queue, function() {
    delete self.socket;
    delete self.pub;

    setImmediate(self.onStop.bind(self));
  });
};

Broker.prototype.onStart = function() {
  if (this.conf.onStart) {
    this.conf.onStart();
  }
  this.emit('start');
};

Broker.prototype.onStop = function() {
  if (this.conf.onStop) {
    this.conf.onStop();
  }
  this.emit('stop');
};

Broker.prototype.send = function(msg) {
  if (!this.socket) {
    return;
  }

  this.socket.send(msg);
};

Broker.prototype.onMsg = function(_msg) {
  var msg = putils.mparse(_msg);

  var header = msg[1];

  if (header == MDP.CLIENT) {
    this.onClient(msg);
  } else if (header == MDP.WORKER) {
    this.onWorker(msg);
  } else {
    this.emitErr('ERR_MSG_HEADER');
  }

  this.workersCheck();
};

Broker.prototype.emitErr = function(msg) {
  this.emit.apply(this, ['error', msg]);
};

Broker.prototype.onClient = function(msg) {
  var clientId = msg[0];
  var type = msg[2];

  var req;
  var rid;
  if (type == MDP.W_REQUEST) {
    var srvName = msg[3] || 'UNK';
    rid = msg[4];
    debug('B: REQUEST from clientId: %s, service: %s', clientId, srvName);

    var opts = msg[6];

    try {
      opts = JSON.parse(opts);
    } catch (e) {
      // Ignore
    }
    if (!_.isObject(opts)) {
      opts = {};
    }

    req = {
      service: srvName,
      clientId: clientId,
      attempts: 0,
      rid: rid,
      timeout: opts.timeout || 60000,
      ts: (new Date()).getTime(),
      rejects: new Map(),
      msg: msg,
      opts: opts
    };

    this.requestProcess(req);
  } else if (type == MDP.W_HEARTBEAT) {
    if (msg.length === 5) {
      var type = msg[3];
      if (type === 'req') {
        rid = req[4];
        req = this.rmap.get(rid);
        if (req && req.workerId) {
          this.send([req.workerId, MDP.WORKER, MDP.W_HEARTBEAT, clientId, '', rid]);
        }
      } else if (type === 'worker') {
        var worker = this.workers.get(msg[4]);
        if (worker) {
          worker.liveness = HEARTBEAT_LIVENESS;
        }
      }
    } else {
      this.send([clientId, MDP.CLIENT, MDP.W_HEARTBEAT]);
    }
  }
};

Broker.prototype.onWorker = function(msg) {
  var workerId = msg[0];
  var type = msg[2];

  var wready = this.workers.has(workerId);
  var worker = this.workerRequire(workerId);

  var opts;


  if (type == MDP.W_READY) {
    var srvName = msg[3];

    debug('B: register worker: %s, service: %s', workerId, srvName, wready ? 'R' : '');

    if (!srvName) {
      this.workerDelete(workerId, true);

    } else if (!wready) {
      this.serviceRequire(srvName);
      this.serviceWorkerAdd(srvName, workerId);

      this.send([workerId, MDP.WORKER, MDP.W_READY]);
    }
    return;
  }

  if (!wready) {
    this.workerDelete(workerId, true);
    return;
  }

  worker.liveness = HEARTBEAT_LIVENESS;

  if (type == MDP.W_REPLY || type == MDP.W_REPLY_PARTIAL || type == MDP.W_REPLY_REJECT) {
    var rid = msg[5];

    if (!worker.rids.has(rid)) {
      debug('B: FATAL from worker \'%s\' (%s), rid not found mismatch \'%s\'', workerId, worker.service, rid);
      this.workerDelete(workerId, true);
      return;
    }

    var req = this.rmap.get(rid);
    if (!req) {
      debug('B: FATAL from worker \'%s\' (%s), req not found', workerId, worker.service, rid);
      this.workerDelete(workerId, true);
      return;
    }

    var service = this.serviceRequire(req.service);

    if (type == MDP.W_REPLY_REJECT) {
      debug('B: REJECT from worker \'%s\' (%s) for req \'%s\'', workerId, worker.service, rid);

      req.rejects.set(workerId, 1);
      delete req.workerId;
      worker.rids.delete(rid);

      service.q.push(req.rid);
      this.dispatch(req.service);

    } else if (type == MDP.W_REPLY || type == MDP.W_REPLY_PARTIAL) {
      debug('B: REPLY from worker \'%s\' (%s)', workerId, worker.service);

      opts = msg[8];
      try {
        opts = JSON.parse(opts);
      } catch (e) {
        // Ignore
      }
      if (!_.isObject(opts)) {
        opts = {};
      }

      var obj = msg.slice(6);

      this.reply(type, req, obj);

      if (type == MDP.W_REPLY) {
        worker.rids.delete(rid);
        this.requestDelete(req);
        this.dispatch(req.service);
        if (req.service !== worker.service) {
          this.dispatch(worker.service);
        }
      }
    }

  } else if (type == MDP.W_HEARTBEAT) {
    opts = msg[4];
    try {
      opts = JSON.parse(opts);
    } catch (e) {
      opts = {};
    }

    _.each(['concurrency'], function(fld) {
      if (!_.isUndefined(opts[fld])) {
        worker.opts[fld] = opts[fld];
      }
    });

    worker.liveness++;
    this.send([workerId, MDP.WORKER, MDP.W_HEARTBEAT]);

    if (worker.service) {
      if (worker.service.indexOf('$') === 0 && opts.update) {
        this.notify(worker.service);
      }
    }

  } else if (type == MDP.W_DISCONNECT) {
    this.workerDelete(workerId, true);
  }
};

Broker.prototype.reply = function(type, req, msg) {
  this.send([req.clientId, MDP.CLIENT, type, '', req.rid].concat(msg));
};

Broker.prototype.requestProcess = function(req) {
  var service = this.serviceRequire(req.service);

  this.rmap.set(req.rid, req);
  if (req.opts.persist) {
    this.ctrl.rset(req);
  }

  service.q.push(req.rid);
  this.dispatch(req.service);
};

Broker.prototype.requestDelete = function(req) {
  if (!req) {
    return;
  }

  this.rmap.delete(req.rid);
  if (req.opts.persist) {
    this.ctrl.rdel(req);
  }
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
  if (this.workers.has(workerId)) {
    return this.workers.get(workerId);
  }

  var worker = {
    workerId: workerId,
    liveness: HEARTBEAT_LIVENESS,
    rids: new Map(),
    opts: {
      concurrency: 100
    },
    rcnt: 0
  };

  this.workers.set(workerId, worker);

  return worker;
};

Broker.prototype.workerDelete = function(workerId, disconnect) {
  var self = this;
  
  var worker = this.workers.get(workerId);

  if (!worker) {
    this.workers.delete(workerId);
    return;
  }

  debug('B: Worker delete \'%s\' (%s)', workerId, disconnect);

  if (disconnect) {
    this.send([workerId, MDP.WORKER, MDP.W_DISCONNECT]);
  }

  var service = null;
  var dservices = [];
  if (worker.service) {
    service = this.serviceRequire(worker.service);
    service.workers.delete(workerId);
    dservices.push(worker.service);
  }

  this.workers.delete(workerId);

  worker.rids.forEach(function(__, rid) {
    var req = self.rmap.get(rid);
    
    if (!req) {
      return;
    }

    delete req.workerId;

    var crd = true;

    if (worker.service) {
      if (req.opts.retry) {
        service.q.push(req.rid);
        crd = false;
      }
    }

    if (crd) {
      self.requestDelete(req);
    }

    if (_.indexOf(dservices, req.service) === -1) {
      dservices.push(req.service);
    }
  });

  this.notify('$dir');

  if (dservices.length) {
    for (var s = 0; s < dservices.length; s++) {
      this.dispatch(dservices[s]);
    }
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

  this.workers.forEach(function(worker, workerId) {
    if (!worker) {
      self.workerDelete(workerId, true);
      return;
    }

    worker.liveness--;

    if (worker.liveness < 0) {
      debug('B: Worker purge \'%s\'', workerId);
      self.workerDelete(workerId, true);
      return;
    }

    if (worker.liveness < HEARTBEAT_LIVENESS) {
      self.send([workerId, MDP.WORKER, MDP.W_HEARTBEAT]);
    }
  });
};

Broker.prototype.workerAvailable = function(workerId) {
  if (!workerId) {
    return false;
  }

  var worker = this.workers.get(workerId);

  if (!worker) {
    return false;
  }

  if (worker.opts.concurrency === -1) {
    return true;
  }

  if (worker.rids.size < worker.opts.concurrency) {
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
    workers: new Map(),
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
    if (srvName.indexOf('@') > 0 && !srvName.match(/@\d+[.]\d+[.]\d+$/)) {
      // find all satisfying services

      var prefix = srvName.replace(/@([^@]+)$/, '@');
      var range = srvName.replace(/(^.*@)([^@]+)$/, '$2');
      var satisfying = _.filter(self.services, function(aService, aSrvName) {
        if (aSrvName == srvName || aSrvName.indexOf(prefix) !== 0 || !aSrvName.match(/@\d+[.]\d+[.]\d+$/)) {
          return false;
        }

        var version = aSrvName.replace(/^.+@([^@]+)$/, '$1');
        return semver.satisfies(version, range);
      });

      service.aux = _.pluck(satisfying, 'name');
    }
  });

  _.each(this.services, function(service, srvName) {
    if (!service.workers.size && srvName[srvName.length - 1] !== '*' && srvName.indexOf('@') === -1) {
      // let's find the best matching widlcard services

      var bestMatching = _.reduce(self.services, function(acc, aService, aSrvName) {

        if (aSrvName == srvName || aSrvName[aSrvName.length - 1] !== '*' || srvName.indexOf(aSrvName.slice(0, -1)) !== 0) {
          return acc;
        }

        return aSrvName.length > acc.length ? aSrvName : acc;
      }, '');

      if (bestMatching) {
        service.aux.push(bestMatching);
      }
    }
  });

  _.each(this.services, function(service, srvName) {
    service.aux = _.uniq(service.aux);
    if (!service.aux.length) {
      delete self.services[srvName];
    }
  });
};

Broker.prototype.serviceWorkerAdd = function(srvName, workerId) {
  var service = this.serviceRequire(srvName);
  var worker = this.workerRequire(workerId);

  if (!worker) {
    this.workerDelete(workerId, true);
    return;
  }

  if (!service.workers.has(workerId)) {
    worker.service = srvName;
    service.workers.set(workerId, 1);
  }

  this.notify('$dir');
  this.dispatch(srvName);
};

function _rhandle(srvName, workerIds) {
  var self = this;

  var service = this.serviceRequire(srvName);

  var rid = service.q.shift();
  if (!rid) {
    return 0;
  }

  var req = this.rmap.get(rid);
  if (!req) {
    return 0;
  }

  var vld = this.requestValidate(req);

  if (!vld) {
    this.rmap.delete(req.rid);
    if (req.opts.persist) {
      this.ctrl.rdel(req);
    }

    return service.q.length;
  }

  req.attempts++;

  var workerId = null;
  var wcret = 0;

  if (workerIds.length) {
    if (req.opts.workerId) {
      wcret = 1;
      if (self.workerAvailable(req.opts.workerId)) {
        workerId = req.opts.workerId;
      }
    } else {
      for (var wi = 0; wi < workerIds.length; wi++) {
        var _workerId = workerIds[wi];
        if (self.workerAvailable(_workerId)) {
          wcret++;
          if (!req.rejects.has(workerId)) {
            workerId = _workerId;
            break;
          }
        }
      }
    }
  }

  if (!workerId) {
    service.q.push(req.rid);
    return wcret;
  }

  var worker = this.workerRequire(workerId);

  req.workerId = worker.workerId;
  worker.rids.set(req.rid, req);
  worker.rcnt++;

  if (req.opts.persist) {
    this.ctrl.rset(req);
  }

  var obj = [
    worker.workerId, MDP.WORKER, MDP.W_REQUEST,
    req.clientId, req.service, ''
  ].concat(req.msg.slice(4));

  this.send(obj);

  return 1;
}

Broker.prototype.dispatch = function(srvName) {
  var self = this;
  var service = this.serviceRequire(srvName);
  var qlen = service.q.length;

  if (!qlen) {
    return;
  }

  var workerIds = [];
  for (var s = 0; s < service.aux.length; s++) {
    var aService = this.serviceRequire(service.aux[s]);
    workerIds = workerIds.concat(Array.from(aService.workers.keys()));
  }

  if (this.conf.dmode === 'load' && workerIds.length > 1) {
    workerIds.sort(function(a, b) {
      var wa = self.workers.get(a);
      var wb = self.workers.get(b);
      var arn = wa.rids.size;
      var brn = wb.length;

      if (arn < brn) {
        return -1;
      } else if (brn < arn) {
        return 1;
      }

      return wa.rcnt <= wb.rcnt ? -1 : 1;
    });
  } else if (this.conf.dmode === 'rand' && workerIds.length > 1) {
    workerIds = putils.shuffle(workerIds);
  }

  for (var r = 0; r < qlen; r++) {
    var ret = _rhandle.call(this, srvName, workerIds);
    if (!ret) {
      break;
    }
  }
};

Broker.prototype.notify = function(channel) {
  switch (channel) {
    case '$dir':
      this.pub.send('$dir ' + JSON.stringify(
        _.reduce(this.services, function(acc, service, srvName) {
          acc[srvName] = Array.from(service.workers.keys());
          return acc;
        }, {})
      ));
      break;
  }
};

module.exports = Broker;
