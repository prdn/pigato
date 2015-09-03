var fs = require('fs');
var Client = require('./../../index').Client;

var conf = JSON.parse(fs.readFileSync(__dirname + '/../config.json', 'UTF-8'));

var client = new Client(conf.broker.host);
client.start();

client.on('error', function(e) {
  if (e === 'ERR_REQ_INVALID') {
    inv++;
  }
  console.log('ERROR', e);
});

var d1 = new Date();
var reqs = 100000;

var rcnt = 0;
var inv = 0;

for (var i = 0; i < reqs; i++) {
  client.request(
    'echo', 'foo', 
    function(err, data) {},
    function(err, data) {
      rcnt++;
      if (rcnt === reqs) {
        console.log(reqs + ' requests/replies processed (' + ((new Date()).getTime() - d1.getTime()) + ' milliseconds)', 'inv=' + inv);
        process.exit(0);
      }
    }, { timeout: 10000 }
  );
}
