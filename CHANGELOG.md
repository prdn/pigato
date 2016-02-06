# PIGATO CHANGELOG

### v0.0.45
* Minor fix

### v0.0.44
* Improved internal structures.
* Added support for Worker liveness refresh via Client.

### v0.0.43
* Js lint
* BREAKING: Worker and Client now must specificy 'prefix' instead of 'name'. The socket identifier is generated using 'prefix' followed by a random uuid. This fixes issues in socket identifier overlapping.
* Support for Worker semver

### v0.0.42
* Upgraded to zmq-2.13.0 to support iojs >= 3.0
* Removed cache support from Broker (shoulbe be refactored)
* Broker minor refactoring
* Switched to use an improved performance queue library

### v0.0.41
* Minor performance improvements
* Test fixes

### v0.0.40
* Worker can see Client request options
* Broker stop timeout (flush sockets)
* Fix Worker call reply.end() without arguments
* Added error emitter when Broker receives invalid messages
* Worker heartbeating logic improvement

### v0.0.39
* Minor perf improvements
* Load and rand broker policies
* Wildcard fixes (maxired)
* Symmetric behaviour for Client/Worker/Broker (maxired)
* Add Broker onStart/onStop callbacks and start/stop events (maxired)
* Add Worker onConnect/onDisconnect callbacks and connect/disconnect/start/stop events (maxired)
* Add Client onConnect/onDisconnect callbacks and connect/disconnect/start/stop events (maxired)
* More tests (maxired)
* A bit of refactoring

### v.0.0.38
* Minor perf improvements 

### v.0.0.37
* Minor changes 

### v.0.0.36
* Minor changes 

### v.0.0.35 
* Core Services exported
* Core Services documentation

### v.0.0.34
* Test suite refactoring for improved execution speed
* Support for targeting a Client Request to a specific Worker using the Request option workerId
* Minor Broker code refactoring
* Broker dispatcher improvements
* Refactored test directory structure
* New test for file descriptors management 
* Fixed Broker internal request-map memory leak

### v.0.0.33
* Minor fixes
* Stress test for file descriptors
* Changelog moved to its own file 

### v.0.0.32
* Minor fix in `client.requestStream`
* Changelog and protocol specs

### v.0.0.31
* Support for `opts.nocache` flag in `client.request` : Client requests a fresh uncached reply

