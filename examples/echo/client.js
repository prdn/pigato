var fs = require('fs');
var Client = require('./../../index').Client;

var conf = JSON.parse(fs.readFileSync(__dirname + '/../config.json', 'UTF-8'));

var client = new Client('tcp://' + conf.broker.host + ':' + conf.broker.port);
client.start();

client.on('error', function(e) {
  console.log('ERROR', e);
});

// STREAM MODE 
console.log("CLIENT SEND REQUEST (stream mode)");
client.request(
  'echo', 'foo-stream', 
  { timeout: 10000 }
)
.on('data', function(data) {
  console.log("DATA", data);	
})
.on('end', function() {
  console.log("END");	  
});

// CALLBACK MODE
console.log("CLIENT SEND REQUEST (callback mode)");
client.request(
  'echo', 'foo-callback', 
  function(err, data) {
    console.log("PARTIAL", err, data);
  },
  function(err, data) {
    console.log("FINAL", err, data);
  }, { timeout: 10000 }
);
