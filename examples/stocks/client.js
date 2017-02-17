var Client = require('./../../index').Client;
var conf = require('../config.json');

var client = new Client(conf.broker.host);
client.start();

client.on(
  'error',
  function(err) {
    console.log("CLIENT ERROR", err);
  }
);

// Streaming implementation

var res = client.request(
  'stock',
  {
    ticker:'AAPL',
    startDay:'1',
    startMonth:'6',
    startYear:'2013',
    endDay:'1',
    endMonth:'6',
    endYear:'2014',
    freq:'d'
  },
  { timeout: 90000 }
);

var body = '';
res.on('data', function(data) {
  body += data;
}).on('end', function() {
  console.log(body);
});
