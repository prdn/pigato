var http = require('http');
var fs = require('fs');
var Worker = require('./../../index').Worker;

var conf = JSON.parse(fs.readFileSync(__dirname + '/../config.json', 'UTF-8'));

var worker = new Worker('tcp://' + conf.broker.host + ':' + conf.broker.port, 'stock')
worker.start();

worker.on('error',function(err) {
  console.log("WORKER ERROR", err);
});

worker.on('request',function(inp, rep){
  var url="http://ichart.finance.yahoo.com/table.csv?s=" + inp.ticker + 
  "&a=" + inp.startDay + "&b=" + inp.startMonth+"&c=" + inp.startYear + "&d=" +
    inp.endYear + "&e=" + inp.endMonth + "&f=" + inp.endYear + "&g=" + inp.freq + "&ignore=.csv";

  //console.log(url);
  http.get(url, function(res) {
    res.on('data', function(chunk) {
      rep.write(String(chunk));
    }).on('end', function() {
      rep.end('');
    });
  }).on('error', function(e) {
    console.log("Got error: " + e.message);
  });
});
