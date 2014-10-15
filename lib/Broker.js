var util = require('util');
var debug = require('debug')('pigato:Broker');
var uuid = require('shortid');
var events = require('events');
var zmq = require('zmq');
var MDP = require('./mdp');

var HEARTBEAT_LIVENESS = 3;

function Broker(endpoint, conf) {
    this.endpoint = endpoint;

    this.services = {};
    this.workers = {};
	this.reqs = {};

	this.conf = {
		heartbeat: 2500
	};
	
	var self = this;

	Object.keys(conf || {}).every(function(k) {
		self.conf[k] = conf[k];
	});

    events.EventEmitter.call(this);
}
util.inherits(Broker, events.EventEmitter);

Broker.prototype.start = function(cb) {
    var self = this;

	this.name = 'B' + uuid.generate();
    this.socket = zmq.socket('router');
    this.socket.identity = new Buffer(this.name);

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
		self.workerPurge();

		Object.keys(self.workers).every(function(workerId) {
			var worker = self.workerRequire(workerId);
			self.send([workerId, MDP.WORKER, MDP.W_HEARTBEAT]);
			return true;
		});
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
    var header = msg[1].toString();

	if (header == MDP.CLIENT) {
        this.onClient(msg);
    } else if (header == MDP.WORKER) {
        this.onWorker(msg);
    } else {
		debug('(onMsg) Invalid message header \'' + header + '\'');
    }
};

Broker.prototype.emitErr = function(msg) {
    this.emit.apply(this, ['error', msg]);
};

Broker.prototype.onClient = function(msg) {
	var clientId = msg[0].toString();
	var type = msg[2];
	
	if (type == MDP.W_REQUEST) {
		var serviceName = msg[3].toString();
		//debug("B: REQUEST from clientId: %s, service: %s", clientId, serviceName);
		this.requestQueue(serviceName, msg);
	} else if (type == MDP.W_HEARTBEAT) {
		if (msg.length == 4) {
			var rid = msg[3].toString();
			var req = this.reqs[rid];
			if (req && req.workerId) {
				this.send([req.workerId, MDP.WORKER, MDP.W_HEARTBEAT, clientId, '', rid]);
			}
		}
	}
};

Broker.prototype.onWorker = function(msg) {
	var workerId = msg[0].toString();
	var type = msg[2];

	var workerReady = (workerId in this.workers);
	var worker = this.workerRequire(workerId);
	
	worker.liveness = HEARTBEAT_LIVENESS;
	
	if (type == MDP.W_READY) {
		serviceName = msg[3].toString();

		debug('B: register worker: %s, service: %s', workerId, serviceName, workerReady ? 'R' : '');

		if (workerReady) {
			this.workerDelete(workerId, true);

		} else {
			this.workerSetService(workerId, serviceName);
			this.workerWaiting(workerId);
		}

		return;
	} 

	if (!workerReady) {
		this.workerDelete(workerId, true);
		return;
	}

	if (type == MDP.W_REPLY || type == MDP.W_REPLY_PARTIAL) {
		var clientId = msg[3].toString();
		var rid = msg[4].toString();

		debug("B: REPLY from worker '%s' (%s)", workerId, worker.service.name);

		var obj = [clientId, MDP.CLIENT, type];
		for (var i = 4; i < msg.length; i++) {
			obj.push(msg[i]);
		}

		this.send(obj);
		if (type == MDP.W_REPLY) {
			this.workerWaiting(workerId);
		}

	} else {
		if (type == MDP.W_DISCONNECT) {
			this.workerDelete(workerId, false);
		}
	}
};

Broker.prototype.requestDelete = function(rid) {
	var req = this.reqs[rid];
	if (!req) {
		return;
	}
		
	delete this.reqs[rid];
};

Broker.prototype.requestQueue = function(serviceName, msg) {
	var clientId = msg[0].toString();
	var rid = msg[4].toString();
	var opts = msg[6] ? msg[6].toString() : null;

	try { 
		opts = JSON.parse(opts) || {};
	} catch(e) {
		opts = null;
	}

	if (typeof opts != 'object') {
		opts = {};
	}

	var req = this.reqs[rid] = {
		clientId: clientId,
		rid: rid,
		timeout: opts.timeout || 60000,
		ts: (new Date()).getTime(),
		msg: msg,
		opts: opts
	};

	var service = this.serviceRequire(serviceName);
	service.requests.push(req);

	this.serviceDispatch(serviceName);
};

Broker.prototype.requestUnqueue = function(serviceName) {
	var service = this.serviceRequire(serviceName);

	while (service.requests.length) {
		return service.requests.shift();
	}

	return null;
};

Broker.prototype.requestValidate = function(rid) {
	var req = this.reqs[rid];

	if (!req) {
		return false;
	}

	if (req.workerId) {
		return true;
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

Broker.prototype.workerSetService = function(workerId, serviceName) {
	var worker = this.workerRequire(workerId);
	worker.service = this.serviceRequire(serviceName);
	worker.service.workers++;
};

Broker.prototype.workerUnsetService = function(workerId) {
	var worker = this.workerRequire(workerId);
	
	if (!worker.service) {
		return;
	}

	for (var i = 0; i < worker.service.waiting.length; i++) {
		if (workerId != worker.service.waiting[i]) {
			continue;
		}
	
		worker.service.waiting.splice(i, 1);
	
		break;
	}
	worker.service.workers--;
};

Broker.prototype.workerDelete = function(workerId, disconnect) {
    var worker = this.workerRequire(workerId);

	if (!worker) {
		return;
	}
    
	debug('workerDelete \'%s\'%s (%s)', workerId, disconnect);

	if (disconnect) {
        this.send([workerId, MDP.WORKER, MDP.W_DISCONNECT]);
    }

    // remove worker from service's waiting list
	this.workerUnsetService(workerId);
    
    // remove worker from broker's workers list
    delete this.workers[workerId];
};

Broker.prototype.workerGetServiceName = function(workerId) {
    var worker = this.workers[workerId];
    return worker.service.name;
};

Broker.prototype.workerWaiting = function(workerId) {
	var worker = this.workerRequire(workerId);

	if (worker.service.waiting.indexOf(workerId) > -1) {
		this.workerDelete(workerId, true);
		return;
	}

    // add worker to waiting list
	worker.service.waiting.push(workerId);

	var self = this;
	process.nextTick(function() {
		self.serviceDispatch(worker.service.name);
	});
};

Broker.prototype.workerPurge = function() {
    var self = this;

	Object.keys(this.workers).every(function(workerId) {
		var worker = self.workers[workerId];

		worker.liveness--;
		if (worker.liveness < 0) {
			debug('B: workerPurge \'%s\'', workerId);
			self.workerDelete(workerId, true);
		}

		return true;
	});
};

Broker.prototype.serviceRequire = function(name) {
    if (name in this.services) {
        return this.services[name];
    }

    var service = {
        name: name,
        requests: [],
        waiting: [],
        workers: 0
    };
    
    this.services[name] = service;
    return service;
};

Broker.prototype.serviceWorkerUnwait = function(serviceName) {
	var service = this.serviceRequire(serviceName);
	if (service.waiting.length) {
		return service.waiting.shift();
	}
	return null;
};

Broker.prototype.serviceDispatch = function(serviceName) {
	var service = this.serviceRequire(serviceName);

    while (service.waiting.length && service.requests.length) {
        
		// get idle worker
		var workerId = this.serviceWorkerUnwait(serviceName);
		if (!workerId) {
			continue;
		}

		var worker = this.workerRequire(workerId);

        // get next request
		var req = this.requestUnqueue(serviceName);
		if (!this.requestValidate(req.rid)) {
			this.requestDelete(req.rid);
			this.workerWaiting(workerId);
			continue;
		}

		req.workerId = workerId;

		var obj = [
			workerId, MDP.WORKER, MDP.W_REQUEST, req.clientId, '' 
		];

		for (var i = 4; i < req.msg.length; i++) {
			obj.push(req.msg[i]);
		}
	
		this.send(obj);	
    }

	debug('B: serviceDispatch: idle requests=%s, workers=%s', service.requests.length, service.waiting.length);
};

module.exports = Broker;
