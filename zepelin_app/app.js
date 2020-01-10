'use strict';
const http = require('http');
var assert = require('assert');
const express= require('express');
const app = express();
const mustache = require('mustache');
const filesystem = require('fs');
const url = require('url');
const hbase = require('hbase-rpc-client');
const hostname = '127.0.0.1';
const port = 3526;

var client = hbase({
    zookeeperHosts: ["mpcs53014c10-m-6-20191016152730.us-central1-a.c.mpcs53014-2019.internal:2181"],
    zookeeperRoot: "/hbase-unsecure"
});

client.on('error', function(err) {
  console.log(err)
})

// replace delays with yearlycombos
// replace carrier by year
app.use(express.static('public'));

app.get('/yearlycombos.html',function (req, res) {
    const year = req.query['year'];
    console.log(year);
    const get = new hbase.Get(year);
    client.get("zepelin_hbase_yearly_combos", get, function(err, row) {
	assert.ok(!err, "get returned an error: #{err}");
	if(!row){
            res.send("<html><body>No such year in data</body></html>");
            return;
        }

	function weather_incidents(weather) {
	    var incidents = row.cols["year:" + weather].value;
	    if(incidents == 0)
		return " - ";
	    return incidents; 
	}

	var template = filesystem.readFileSync("result.mustache").toString();
	var html = mustache.render(template,  {
	    year : req.query['year'],
	    totalincidents : weather_incidents("totalincidents"),
	    LWLTLP : weather_incidents("LWLTLP"),
	    LWLTHP : weather_incidents("LWLTHP"),
	    LWMTLP : weather_incidents("LWMTLP"),
	    LWMTHP : weather_incidents("LWMTHP"),
	    LWHTLP : weather_incidents("LWHTLP"),
	    LWHTHP : weather_incidents("LWHTHP"),
	    MWLTLP : weather_incidents("MWLTLP"),
	    MWLTHP : weather_incidents("MWLTHP"),
	    MWMTLP : weather_incidents("MWMTLP"),
	    MWMTHP : weather_incidents("MWMTHP"),
	    MWHTLP : weather_incidents("MWHTLP"),
	    MWHTHP : weather_incidents("MWHTHP"),
	    HWLTLP : weather_incidents("HWLTLP"),
	    HWLTHP : weather_incidents("HWLTHP"),
	    HWMTLP : weather_incidents("HWMTLP"),
	    HWMTHP : weather_incidents("HWMTHP"),
	    HWHTLP : weather_incidents("HWHTLP"),
	    HWHTHP : weather_incidents("HWHTHP")
	});
	res.send(html);
    });
});
// Second View
app.get('/perday.html',function (req, res) {
    const year = req.query['year'];
    console.log(year);
    const get = new hbase.Get(year);
    client.get("zepelin_hbase_crimes_by_day", get, function(err, row) {
	assert.ok(!err, "get returned an error: #{err}");
	if(!row){
            res.send("<html><body>No such year in data</body></html>");
            return;
        }

	function daily_incidents(weather) {
	    var incidents = row.cols["year:" + weather + "incidents"].value;
	    var days = row.cols["year:" + weather + "days"].value;
	    if(days == 0)
		return " N/A ";
	    return (incidents / days).toFixed(1);; 
	}

	var template = filesystem.readFileSync("result2.mustache").toString();
	var html = mustache.render(template,  {
	    year : req.query['year'],
	    all : daily_incidents("all"),
	    lowwind : daily_incidents("lowwind"),
	    moderatewind : daily_incidents("moderatewind"),
	    highwind : daily_incidents("highwind"),
	    lowtemp : daily_incidents("lowtemp"),
	    moderatetemp : daily_incidents("moderatetemp"),
	    hightemp : daily_incidents("hightemp"),
	    lowprecip : daily_incidents("lowprecip"),
	    highprecip : daily_incidents("highprecip")
	});
	res.send(html);
    });
});

// Third View
app.get('/arrestperday.html',function (req, res) {
    const year = req.query['year'];
    console.log(year);
    const get = new hbase.Get(year);
    client.get("zepelin_hbase_crimes_by_day", get, function(err, row) {
	assert.ok(!err, "get returned an error: #{err}");
	if(!row){
            res.send("<html><body>No such year in data</body></html>");
            return;
        }

	function daily_incidents(weather) {
	    var incidents = row.cols["year:" + weather + "incidents"].value;
	    var arrests = row.cols["year:" + weather + "arrests"].value;
	    if(incidents == 0)
		return " N/A ";
	    return (100 * arrests / incidents).toFixed(1);; 
	}

	var template = filesystem.readFileSync("result3.mustache").toString();
	var html = mustache.render(template,  {
	    year : req.query['year'],
	    all : daily_incidents("all"),
	    lowwind : daily_incidents("lowwind"),
	    moderatewind : daily_incidents("moderatewind"),
	    highwind : daily_incidents("highwind"),
	    lowtemp : daily_incidents("lowtemp"),
	    moderatetemp : daily_incidents("moderatetemp"),
	    hightemp : daily_incidents("hightemp"),
	    lowprecip : daily_incidents("lowprecip"),
	    highprecip : daily_incidents("highprecip")
	});
	res.send(html);
    });
});
	

app.get('/perday.html',function (req, res) {
    const year = req.query['year'];
    console.log(year);
    const get = new hbase.Get(year);
    client.get("zepelin_hbase_crimes_by_day", get, function(err, row) {
	assert.ok(!err, "get returned an error: #{err}");
	if(!row){
            res.send("<html><body>No such year in data</body></html>");
            return;
        }

	function daily_incidents(weather) {
	    var incidents = row.cols["year:" + weather + "incidents"].value;
	    var days = row.cols["year:" + weather + "days"].value;
	    if(days == 0)
		return " N/A ";
	    return (incidents / days).toFixed(1);; 
	}

	var template = filesystem.readFileSync("result2.mustache").toString();
	var html = mustache.render(template,  {
	    year : req.query['year'],
	    all : daily_incidents("all"),
	    lowwind : daily_incidents("lowwind"),
	    moderatewind : daily_incidents("moderatewind"),
	    highwind : daily_incidents("highwind"),
	    lowtemp : daily_incidents("lowtemp"),
	    moderatetemp : daily_incidents("moderatetemp"),
	    hightemp : daily_incidents("hightemp"),
	    lowprecip : daily_incidents("lowprecip"),
	    highprecip : daily_incidents("highprecip")
	});
	res.send(html);
    });
});

// Submitting Data
app.get('/submit.html',function (req, res) {
    var wind = req.query['wind'];
    var temp = req.query['temp'];
    var precip = req.query['precipitation'];
    var arrest = req.query['arrest'];
    var combo = wind.concat(temp, precip)
    console.log(wind);
    console.log(temp);
    console.log(precip);
    console.log(combo)
    console.log(arrest);

    var kafka = require('kafka-node'),
	    Producer = kafka.Producer,
	    KeyedMessage = kafka.KeyedMessage,
	    client = new kafka.KafkaClient('mpcs53014c10-m-6-20191016152730.us-central1-a.c.mpcs53014-2019.internal:6667'),
	    producer = new Producer(client),
	    km = new KeyedMessage('2019', combo),
	    payloads = [
	        { topic: 'zepelin_combos', messages: [km] }
	    ];
	    client.on('ready', function (){
        	console.log('client ready');
    	})  
    	client.on('error', function (err){
        	console.log('client error: ' + err);
    	})  
    	producer.on('ready', function () {
        	producer.send(payloads, function (err, data) {
            	console.log('send: ' + data);        
            	process.exit();
        	});
    	});
    	producer.on('error', function (err) {
        	console.log('error: ' + err);
        	process.exit();
    	});
	});
app.listen(port);
