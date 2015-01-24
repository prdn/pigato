PIGATO
========

[![PIGATO](http://ardoino.com/pub/pigato-200.png)](https://github.com/prdn/pigato)

**PIGATO - a microservices framework for Node.js based on ZeroMQ**

The goal is to offer a reliable and extensible service-oriented request-reply inspired by [Majordomo Protocol (MDP) v0.2](http://rfc.zeromq.org/spec:7) and [Titanic Service Protocol](http://rfc.zeromq.org/spec:9). 

#### Structure
* Worker : receives requests, does something and replies. A worker offers a service, should be a functionality as atomic as possible
* Client : creates, pushes requests and waits for results (if needed). A request always includes a service and a payload/data for the Worker
* Broker : handles requests queueing and routing

#### Examples

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
node examples/echo/worker
node examples/echo/client
```


More examples

[PIGATO-EXAMPLES](https://github.com/fincluster/pigato-examples) : a collection of multi-purpose useful examples.

#### Performance

[PIGATO-PERF](https://github.com/prdn/pigato-perf) : a command-line tool to test PIGATO performances in different scenarios.

### API

#### `pigato.Broker(addr)`
* `addr` - Broker address (string, i.e: 'tcp://*:12345') 

Simply starts up a broker.

```
var Broker = require('./../index').Broker;

var broker = new Broker("tcp://*:55555");
broker.start(function(){});
```

#### `pigato.Worker(addr, serviceName, conf)`
* `addr` - Broker address (string, i.e: 'tcp://localhost:12345') 
* `serviceName` - service implemented by the Worker (string, i.e: 'echo')
* `conf` - configuration override (object: { concurrency: 20 })
  * `concurrency` - sets max number of concurrent requests (-1 = no limit)

Worker receives `"request"` events with 2 arguments:

* `data` - value sent by a client for this request.
* `reply` - extended writable stream to send data to the client.

`reply` writable stream exposes also following methods and attributes:

* `write()` - sends partial data to the client (triggers partial callback). It is used internally to implement writable streams.
* `end()` - sends last data to the client (triggers final callback) and completes/closes current request. Use this method for single-reply requests.
* `reject()` - rejects a request.
* `heartbeat()` - forces sending heartbeat to the broker and client
* `active()` - returns (boolean) the status of the request. A request becomes inactive when the worker disconnects from the broker or it is discarded by the client or the client disconnects from the broker. This is useful for long running tasks and Worker can monitor whether or not continue processing a request.
* `ended` - tells (boolean) if the request has been ended.

Example:
```
var worker = new PIGATO.Worker('tcp://localhost:12345', 'my-service');

worker.on('request', function(data, reply) {
  fs.createReadStream(data).pipe(reply);
});

// or
worker.on('request', function(data, reply) {
  for (var i = 0; i < 1000; i++) {
    reply.write('PARTIAL DATA ' + i);
  }
  reply.end('FINAL DATA');
});
```

Worker may also specify whether the reply should be cached and the cache timeout in milliseconds 

Example:
```
worker.on('request', function(data, reply) {
  reply.opts.cache = 1000; // cache reply for 1 second
  reply.end('FINAL DATA');
});
```

Worker can change concurrency level updating its configuration. This information is carried with the heartbeat message.

Example:
```
worker.conf.concurrency = 2;
```

Take note: due to the framing protocol of `zmq` only the data supplied to `response.end(data)` will be given to the client's final callback.

#### `PIGATO.Client(addrs)`
* `addrs` - list of Broker addresses (array)

Clients may make requests using `Client.request(...)` method.

* `serviceName` - name of the service we wish to connect to
* `data` - data to give to the service (string/object/buffer)
* `opts` - options object for the request

Example:
```
var client = new PIGATO.Client('tcp://localhost:12345');

client.request('my-service', 'foo', { timeout: 120000 }).pipe(process.stdout);

// or
client.request('my-service', { foo: 'bar' }, { timeout: 120000 })
.on('data', function(data) {
  console.log("DATA", data);	
})
.on('end', function() {
  console.log("END");	  
});
```

Clients may also make request with partial and final callbacks instead of using streams.

* `serviceName`
* `data`
* `partialCallback(err, data)` - called whenever the request does not end but emits data
* `finalCallback(err, data)` - called when the request will emit no more data
* `opts`

Example:
```
client.request('my-service', 'foo', function (err, data) {
  // frames sent prior to final frame
  console.log('PARTIAL', data);
}, function (err, data) {
  // this is the final frame sent
  console.log('FINAL', data);
}, { timeout: 30000 });

```

##### Request options
* `timeout` : timeout in milliseconds; number [default 60000] (-1 to disable = time unlimited request)
* `retry` : if a Worker dies before replying, the Request is automatically requeued. number 0|1 [default 0]

#### Notes
* when using a `inproc` socket the broker *must* become active before any queued messages.

### Protocol

#### Benefits
* Reliable request / reply protocol
* Scalability
* Multi-Worker : infinite services and infinite workers for each service
* Multi-Client : infinite clients
* Multi-Broker : infinite brokers to avoid bottlenecks and improve network reliability

#### Features
* Support for partial replies.
* Client multi-request support.
* Worker concurrent requests.
* Client heartbeating for long running requests. Allows Workers to dected whenever Clients disconnect or lose interest in some request. This feature is very useful to stop long-running partial requests (i.e data streaming).

#### Specification (good for RFC)
* Worker <-> Broker heartbeating.
* Broker MAY track Worker/Client/Request relation.
* Client MAY send heartbeat for active request. If the request is being processed by Worker, Broker forwards heartbeat to Worker. 
* Worker MAY decide to stop an inactive Request (tracks liveness for Request).
* Client MAY assign a timeout to a Request.
* Worker SHALL NOT send more W_REPLY (for a Request) after sending first W_REPLY message.
* Broker SHALL force disconnect Broker if any error occurs.

#### Roadmap
* Add authentication support through [zmq-zap](https://github.com/msealand/zmq-zap.node) ZeroMQ ZAP to trust Clients and Workers.
* Support [Titanic Service Protocol](http://rfc.zeromq.org/spec:9) for peristent requests.

#### Follow me

* Fincluster - cloud financial platform : [fincluster](http://fincluster.com) /  [@fincluster](https://twitter.com/fincluster)
* My personal blog : [ardoino.com](http://ardoino.com) / [@paoloardoino](https://twitter.com/paoloardoino)

#### Contributors
* [bmeck](https://github.com/bmeck)
* [maxired](https://github.com/maxired)
