PIGATO
========

Checkout **[Grenache](https://github.com/bitfinexcom/grenache)** , a DHT based high-performance microservices framework for Node.js, Ruby and Go. Supports ZeroMQ and WebSocket transports.

**PIGATO - an high-performance Node.js microservices framework based on ZeroMQ**

PIGATO aims to offer an high-performance, reliable, scalable and extensible service-oriented framework supporting multiple programming languages: Node.js/Io.js and Ruby.

[![Travis Build Status](https://travis-ci.org/prdn/pigato.svg?style=flat)](https://travis-ci.org/prdn/pigato)
[![NPM version](http://img.shields.io/npm/v/pigato.svg?style=flat)](https://www.npmjs.com/package/pigato)

**Supported Programming Languages**

* [PIGATO](https://github.com/prdn/pigato) : PIGATO Client/Worker/Broker for Node.js / Io.js
* [PIGATO-RUBY](https://github.com/prdn/pigato-ruby) : PIGATO Client/Worker for Ruby
* [PIGATO-GO](https://github.com/prdn/pigato-go) : PIGATO Client for Go


## Structure and Protocol

### Actors
* Worker : receives requests, does something and replies. A Worker offers a Service, should be a functionality as atomic as possible
* Client : creates, pushes Requests and waits for results. A request always includes a service name and data for the Worker
* Broker : handles Requests queueing and routing

### Benefits
* High-performance
* Realiable, Distributed and Scalable
* Load Balancing
* No central point of failure
* Multi-Worker : infinite Services and infinite Workers for each Service
* Multi-Client : infinite Clients
* Multi-Broker : infinite Brokers to avoid bottlenecks and improve network reliability

### Features
* Request / Reply protocol
* Support for partial Replies
* Client concurrent Requests
* Client streaming Requests
* Worker concurrent Requests
* Worker dynamic load balancing
* Client heartbeating for long running requests. Allows Workers to dected whenever Clients disconnect or lose interest in some request. This feature is very useful to stop long-running partial requests (i.e data streaming).

## Examples

Start a **Broker**
```
node examples/broker
```

1) **echo** : simple echo request-reply
```
node examples/echo/worker
node examples/echo/client
```

2) **stocks** : get stocks data from yahoo
```
node examples/stocks/worker
node examples/stocks/client
```


More examples

[PIGATO-EXAMPLES](https://github.com/prdn/pigato-examples) : a collection of multi-purpose useful examples.

### Performance

[PIGATO-PERF](https://github.com/prdn/pigato-perf) : a command-line tool to test PIGATO performances in different scenarios.
i
## API

### Broker
#### `PIGATO.Broker(addr, conf)`
* `addr` - Broker address (string, i.e: 'tcp://*:12345') 
* `conf` - configuration override (type=object, i.e { concurrency: 20 })
  * `onStart`: function to be called when the Broker start 
  * `onStop`: function to be called when the Broker stop

Simply starts up a broker.

```
var Broker = require('./../index').Broker;

var broker = new Broker("tcp://*:55555");
broker.start(function() {
  console.log("Broker started");
});
```

#### Events
* `start` : on Client start
* `stop` : on Client stop

### Worker
#### `PIGATO.Worker(addr, serviceName, conf)`
* `addr` - Broker address (type=string, i.e: 'tcp://localhost:12345') 
* `serviceName` - service implemented by the Worker (type=string, i.e: 'echo')
  * wildcards * are supported (i.e: 'ech*')
* `conf` - configuration override (type=object, i.e { concurrency: 20 })
  * `prefix` - sets the Worker identifier prefix
  * `concurrency` - sets max number of concurrent requests (type=int, -1 = no limit)
  * `onConnect`: function to be called when the Worker connects to the Broker
  * `onDisconnnect`: function to be called when the Worker disconnects from the Broker

#### Methods

##### `on`
Worker receives `request` events with 3 arguments:
* `data` - data sent from the Client (type=string/object/array).
* `reply` - extended writable stream (type=object)
* `opts` - client request options (type=object)

`reply` writable stream exposes also following methods and attributes:

* `write()` - sends partial data to the Client 
* `end()` - sends last data to the Client and completes/closes current Request
* `reject()` - rejects a Request.
* `heartbeat()` - forces sending heartbeat to the Broker
* `active()` - returns the status of the Request (type=boolean). A Request becomes inactive when the Worker disconnects from the Broker or it has been discarded by the Client or the Client disconnects from the Broker. This is useful for long running tasks so the Worker can monitor whether or not continue processing a Request.
* `ended` - tells if the Request has been ended (type=boolean).

**Example**

```
var worker = new PIGATO.Worker('tcp://localhost:12345', 'my-service');
worker.start();

worker.on('request', function(data, reply, copts) {
  for (var i = 0; i < 1000; i++) {
    reply.write('PARTIAL DATA ' + i);
  }
  reply.end('FINAL DATA');
});

// or
worker.on('request', function(data, reply, copts) {
  fs.createReadStream(data).pipe(reply);
});
```

Worker can change concurrency level updating its configuration. This information is carried with the heartbeat message.

**Example**

```
worker.conf.concurrency = 2;
```

Take note: due to the framing protocol of `zmq` only the data supplied to `response.end(data)` will be given to the client's final callback.

#### Events
* `start` : on Worker start
* `stop` : on Worker stop
* `connect` : on Worker connect
* `disconnect` : on Worker disconnect


### Client
#### `PIGATO.Client(addr, conf)`
* `addr` - Broker address (type=string, i.e: 'tcp://localhost:12345') 
* `conf`
  * `prefix` - sets the Client identifier prefix
  * `autostart`: automatically starts the Client (type=boolean, default=false)
  * `onConnect`: function to be called when the Client connects to the Broker
  * `onDisconnnect`: function to be called when the Client disconnects from the Broker

#### Methods

##### `start`

Start the Client

##### `request`

Send a Request

* `serviceName` - name of the Service we wish to connect to (type=string)
* `data` - data to give to the Service (type=string/object/buffer)
* `opts` - options for the Request (type=object)
  * `timeout`: timeout in milliseconds (type=number, default=60000, -1 for infinite timeout)
  * `retry`: if a Worker dies before replying, the Request is automatically requeued. (type=number, values=0|1, default=0)
  * `nocache`: skip Broker's cache
  * `workerId`: ID of the Worker that must handle the Request (type=string)


**Example**

```
var client = new PIGATO.Client('tcp://localhost:12345');
client.start()

client.request('my-service', { foo: 'bar' }, { timeout: 120000 })
.on('data', function(data) {
  console.log("DATA", data);	
})
.on('end', function() {
  console.log("END");	  
});

// or
client.request('my-service', 'foo', { timeout: 120000 }).pipe(process.stdout);
```

Clients may also make request with partial and final callbacks instead of using streams.

* `serviceName`
* `data`
* `partialCallback(err, data)` - called whenever the request does not end but emits data
* `finalCallback(err, data)` - called when the request will emit no more data
* `opts`

**Example**

```
client.request('my-service', 'foo', function (err, data) {
  // frames sent prior to final frame
  console.log('PARTIAL', data);
}, function (err, data) {
  // this is the final frame sent
  console.log('FINAL', data);
}, { timeout: 30000 });

```

#### Events
* `start` : on Client start
* `stop` : on Client stop
* `connect` : on Client connect
* `disconnect` : on Client disconnect

### Core Services
Core services are a set of Services that interact with a Broker via a dedicated PUB/SUB channel to extend its core functionalities.

#### Initialization

```
var broker = new PIGATO.Broker(bhost);
var csrv = new PIGATO.services.ExampleCoreService(bhost, {
  intch: broker.conf.intch // internal pub/sub channel                  
});
broker.start();
csrv.start();
```

#### Directory
##### `PIGATO.services.Directory`

Directory service ($dir) replies to Requests with the list of available Workers for a selected service. 

**Example**

```
// Broadcast a message to all Workers that offer 'echo' Service
client.request(
  '$dir', 'echo', undefined, 
  function(err, workers) {
    workers.forEach(function(wid) {
      client.request('echo', 'foo', { workerId: wid });
    });
  }
);
```

### Notes
* when using a `inproc` socket the broker *must* become active before any queued messages.

## Specification (good for RFC)
* Worker <-> Broker heartbeating.
* Broker tracks Worker/Client/Request relation.
* Client MAY send heartbeat for active request. If the request is being processed by Worker, Broker forwards heartbeat to Worker. 
* Worker MAY decide to stop an inactive Request (tracks liveness for Request).
* Client MAY assign a timeout to a Request.
* Worker SHALL NOT send more W_REPLY (for a Request) after sending first W_REPLY message.
* Broker SHALL force disconnect Worker if any error occurs.

## Protocol

#### Common
* Frame 0: Side tag (MDP.CLIENT/MDP.WORKER)
* Frame 1: Message type (MDP.W_REQUEST, MDP.W_REPLY, MDP.W_REPLY_REJECT, ...)
* Frame 2: Service name
* Frame 3: Request ID (uuid)

#### Client request
* Frame 4: JSON encode request data
* Frame 5: JSON encode request options

#### Worker reply
* Frame 4: Numeric status (0=OK, -1=ERROR)
* Frame 5: JSON encode request data / error
* Frame 6: JSON encode request options

## Changelog

[CHANGELOG](CHANGELOG.md)

## Roadmap
* Add authentication support through [zmq-zap](https://github.com/msealand/zmq-zap.node) ZeroMQ ZAP to trust Clients and Workers.

## Follow me

* My personal blog : [ardoino.com](http://ardoino.com) / [@paoloardoino](https://twitter.com/paoloardoino)

## Contributors
* [bmeck](https://github.com/bmeck)
* [maxired](https://github.com/maxired)
* [leonpegg](https://github.com/leonpegg)
