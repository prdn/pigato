# PIGATO CHANGELOG

### dev
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

