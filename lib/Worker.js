var zmq = require('zmq');
var Writable = require('readable-stream').Writable
var debug = require('debug')('pigato:Worker');
var uuid = require('shortid');
var util = require('util');
var events = require('events');
var MDP = require('./mdp');

var HEARTBEAT_LIVENESS = 3;

function Worker(broker, service, conf) {
    this.broker = broker;
    this.service = service;

	this.conf = {
		heartbeat: 2500,
		reconnect: 100,
		name: 'W' + uuid.generate()
	};

	var self = this;

	Object.keys(conf || {}).every(function(k) {
		self.conf[k] = conf[k];
	});
    
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
        this.socket.close();
        delete this['socket'];
    }
};

// Connect or reconnect to broker
Worker.prototype.connectToBroker = function() {
	var self = this;

	clearInterval(this.hbTimer);
    if (this.socket) {
        this.socket.close();
    }
	this.wait();
     
	this.socket = zmq.socket('dealer');
    this.socket.identity = new Buffer(this.conf.name);

    this.socket.on('message', function() {
        self.onMsg.call(self, arguments);
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
            debug('Disconnected from broker - retrying in %s sec(s)...', (self.conf.reconnect / 1000));
            setTimeout(function() {
                self.connectToBroker();
            }, self.conf.reconnect);
			return;
		}
		
		self.heartbeat();
		
		if (self.req) {
			self.req.liveness--;
		}
    }, this.conf.heartbeat);
};

Worker.prototype.send = function(msg) {
	this.socket.send(msg);
};

// process message from broker
Worker.prototype.onMsg = function(msg) {
    var header = msg[0].toString();
    var type = msg[1];

    if (header != MDP.WORKER) {
        this.emitErr('ERR_MSG_HEADER');
        // send error
        return;
    }
	
	this.liveness = HEARTBEAT_LIVENESS;

	var clientId;
	var rid;

    if (type == MDP.W_REQUEST) {
		clientId = msg[2].toString();
		rid = msg[4].toString();
        //debug('W: W_REQUEST:', clientId, rid);
		this.onRequest(clientId, rid, msg[5].toString());
	} else if (type == MDP.W_HEARTBEAT) {
		if (msg.length === 5) {
			clientId = msg[2].toString();
			rid = msg[4].toString();
			if (this.req && this.req.rid === rid) {
				this.req.liveness = HEARTBEAT_LIVENESS;
			}
		}
	} else if (type == MDP.W_DISCONNECT) {
        debug('W: W_DISCONNECT');
        this.connectToBroker();
    } else {
        this.emitErr('ERR_MSG_TYPE_INVALID');
    }
};

Worker.prototype.emitReq = function(input, reply) {
    this.emit.apply(this, ['request', input, reply]);
};

Worker.prototype.emitErr = function(msg) {
    this.emit.apply(this, ['error', msg]);
};

Worker.prototype.onRequest = function(clientId, rid, data) {
    var self = this;

	this.req = { clientId: clientId, rid: rid, liveness: HEARTBEAT_LIVENESS };

	var reply = new Writable({ objectMode: true });
	reply.ended = false;

	var _write = reply.write;
	reply.write = function(chunk, encoding, cb) {
		return _write.call(reply, chunk, encoding, cb);
	};

	reply._write = function(chunk, encoding, cb) {
		if (this.ended) {
			self.replyFinal(clientId, rid, chunk);
		} else {
			self.replyPartial(clientId, rid, chunk);
		}
		cb(null);
	};

	reply.active = function() {
		return self.req && self.req.rid === rid && self.req.liveness > 0;
	};

	reply.heartbeat = function() {
		self.heartbeat();
	};

	var _end = reply.end;
	reply.end = function() {
		reply.ended = true;
		
		var ret = _end.apply(reply, arguments);

		self.wait();
		return ret;
	};

	reply.reject = function(err) {
		self.replyReject(clientId, rid, err);
		self.wait();
	};

	reply.error = function(err) {
		self.replyError(clientId, rid, err);
		self.wait();
	};

	this.emitReq(JSON.parse(data), reply);
};

Worker.prototype.wait = function() {
	delete this.req;
};

Worker.prototype.sendReady = function() {
    this.send([MDP.WORKER, MDP.W_READY, this.service]);
};

Worker.prototype.sendDisconnect = function() {
    this.send([MDP.WORKER, MDP.W_DISCONNECT]);
};

Worker.prototype.heartbeat = function() {
    this.send([MDP.WORKER, MDP.W_HEARTBEAT]);
};

Worker.prototype.replyPartial = function(clientId, rid, data) {
    this.send([MDP.WORKER, MDP.W_REPLY_PARTIAL, clientId, '', rid, 0, JSON.stringify(data)]);
	return true;
};

Worker.prototype.replyFinal = function(clientId, rid, data) {
    this.send([MDP.WORKER, MDP.W_REPLY, clientId, '', rid, 0, JSON.stringify(data)]);
	return true;
};

Worker.prototype.replyReject = function (clientId, rid, err) {
    this.send([MDP.WORKER, MDP.W_REPLY_REJECT, clientId, '', rid, JSON.stringify(err)]);
	return true;
};

Worker.prototype.replyError = function(clientId, rid, err) {
	this.send([MDP.WORKER, MDP.W_REPLY, clientId, '', rid, JSON.stringify(err)]);
	return true;
};

module.exports = Worker;
