var util = require('util');
var debug = require('debug')('pigato:Broker');
var uuid = require('shortid');
var events = require('events');
var zmq = require('zmq');
var async = require('async');
var MDP = require('./mdp');

var HEARTBEAT_LIVENESS = 3;

var noop = function() {};

function Broker(endpoint, conf) {
    this.endpoint = endpoint;

    this.services = {};
    this.workers = {};

	this.conf = {
		heartbeat: 2500
	};
	
	var self = this;

	Object.keys(conf || {}).every(function(k) {
		self.conf[k] = conf[k];
	});

	this.ctrl = new Controller();

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
		debug('(onMsg) Invalid message header \'' + header + '\'');
    }
};

Broker.prototype.emitErr = function(msg) {
    this.emit.apply(this, ['error', msg]);
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
	var workerId = msg[0].toString();
	var type = msg[2];

	var workerReady = (workerId in this.workers);
	var worker = this.workerRequire(workerId)
		
	if (type == MDP.W_READY) {
		var srvName = msg[3].toString();

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

	if (type == MDP.W_REPLY || type == MDP.W_REPLY_PARTIAL) {
		var clientId = msg[3].toString();
		var rid = msg[4].toString();

		debug("B: REPLY from worker '%s' (%s)", workerId, worker.service);

		var obj = [clientId, MDP.CLIENT, type];
		for (var i = 4; i < msg.length; i++) {
			obj.push(msg[i]);
		}

		this.send(obj);

		if (type == MDP.W_REPLY) {
			delete worker.rid;
			this.serviceWorkerWait(worker.service, workerId, true);
			this.ctrl.rdel(rid);
		}

	} else {
		if (type == MDP.W_DISCONNECT) {
			this.workerDelete(workerId);
		}
	}
};

Broker.prototype.requestQueue = function(srvName, msg) {
	var self = this;
	var clientId = msg[0].toString();
	var rid = msg[4].toString();
	var opts = msg[6] ? msg[6].toString() : null;

	try { 
		opts = JSON.parse(opts) || {};
	} catch(e) {
		opts = null;
	}

	if (typeof opts !== 'object') {
		opts = {};
	}

	var req = {
		service: srvName,
		clientId: clientId,
		rid: rid,
		timeout: opts.timeout || 60000,
		ts: (new Date()).getTime(),
		msg: msg,
		opts: opts
	};

	async.series([
		function(next) {
			self.ctrl.rset(rid, req, next);
		},
		function(next) {
			self.ctrl.rpush(srvName, rid, next);
		},
		function(next) {
			self.dispatch(srvName, next);
		}
	]);
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
			if (!self.requestValidate(req)) {
				if (req) {
					self.ctrl.rdel(req.rid);
					rcnt++;
				}
				self.serviceWorkerWait(srvName, workerId);
				next();
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

	if (!queue) {
		return;
	}

	for (var six = 0; six < swcnt; six++) {
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
}

Controller.prototype.rset = function(rid, req, callback) {
	this.reqs[rid] = req;

	if (callback) {
		callback();
	}
};

Controller.prototype.rpush = function(srv, rid, callback) {
	if (!this.srq[srv]) {
		this.srq[srv] = [];
	}

	var srq = this.srq[srv];
	srq.push(rid);

	if (callback) {
		callback();
	}
};

Controller.prototype.rget = function(rid, callback) {
	var self = this;

	setImmediate(function() {
		callback(null, self.reqs[rid]);
	});
};

Controller.prototype.rpop = function(srv, callback) {
	var srq = this.srq[srv];

	if (!srq) {
		callback();
		return;
	}

	this.rget(srq.shift(), callback);
};

Controller.prototype.rdel = function(rid) {
	delete this.reqs[rid];
};

module.exports = Broker;
