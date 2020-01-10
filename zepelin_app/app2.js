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
const BigIntBuffer = require('bigint-buffer');

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
    console.log("Extracting counts for all combos for year: ")
    console.log(year);
    const get = new hbase.Get(year);
    client.get("zepelin_hbase_yearly_combos", get, function(err, row) {
	assert.ok(!err, "get returned an error: #{err}");
	if(!row){
            res.send("<html><body>No such year in data</body></html>");
            return;
        }



	const get2 = new hbase.Get(year);
	client.get("zepelin_hbase_yearly_combos_speed", get2, function(err2, row2) {
		assert.ok(!err2, "get returned an error: #{err}");
		// It is okay if there are no rows returned!
		function weather_speed(weather) {
			if (req.query['year'] === "2019") {
				return Number(BigIntBuffer.toBigIntBE(row2.cols["number:" + weather].value));
			} else {
				return Number(0);
			}
		}

		function weather_incidents(weather) {
		    var incidents = Number(row.cols["year:" + weather].value);
		    return incidents; 
		}

	var template = filesystem.readFileSync("result.mustache").toString();
	var html = mustache.render(template,  {
	    year : req.query['year'],
	    totalincidents : weather_incidents("totalincidents") + weather_speed("totalincidents"),
	    LWLTLP : weather_incidents("LWLTLP") + weather_speed("LWLTLP"),
	    LWLTHP : weather_incidents("LWLTHP") + weather_speed("LWLTHP"),
	    LWMTLP : weather_incidents("LWMTLP") + weather_speed("LWMTLP"),
	    LWMTHP : weather_incidents("LWMTHP") + weather_speed("LWMTHP"),
	    LWHTLP : weather_incidents("LWHTLP") + weather_speed("LWHTLP"),
	    LWHTHP : weather_incidents("LWHTHP") + weather_speed("LWHTHP"),
	    MWLTLP : weather_incidents("MWLTLP") + weather_speed("MWLTLP"),
	    MWLTHP : weather_incidents("MWLTHP") + weather_speed("MWLTHP"),
	    MWMTLP : weather_incidents("MWMTLP") + weather_speed("MWMTLP"),
	    MWMTHP : weather_incidents("MWMTHP") + weather_speed("MWMTHP"),
	    MWHTLP : weather_incidents("MWHTLP") + weather_speed("MWHTLP"),
	    MWHTHP : weather_incidents("MWHTHP") + weather_speed("MWHTHP"),
	    HWLTLP : weather_incidents("HWLTLP") + weather_speed("HWLTLP"),
	    HWLTHP : weather_incidents("HWLTHP") + weather_speed("HWLTHP"),
	    HWMTLP : weather_incidents("HWMTLP") + weather_speed("HWMTLP"),
	    HWMTHP : weather_incidents("HWMTHP") + weather_speed("HWMTHP"),
	    HWHTLP : weather_incidents("HWHTLP") + weather_speed("HWHTLP"),
	    HWHTHP : weather_incidents("HWHTHP") + weather_speed("HWHTHP")
	});
	res.send(html);
	});
    });
});
// Second View
app.get('/perday.html',function (req, res) {
    const year = req.query['year'];
    console.log("Extracting crime rate data for year: ")
    console.log(year);
    const get = new hbase.Get(year);
    client.get("zepelin_hbase_crimes_by_day", get, function(err, row) {
	assert.ok(!err, "get returned an error: #{err}");
	if(!row){
            res.send("<html><body>No such year in data</body></html>");
            return;
        }


    const get2 = new hbase.Get(year);
	client.get("zepelin_hbase_crimes_by_day_speed", get2, function(err2, row2) {
		assert.ok(!err2, "get returned an error: #{err}");


	function daily_incidents(weather) {
	    var incidents = row.cols["year:" + weather + "incidents"].value;
	    var days = row.cols["year:" + weather + "days"].value;
	    if ( year === "2019"){
	    	var speed_incidents = Number(BigIntBuffer.toBigIntBE(row2.cols["year:" + weather + "incidents"].value));
	    }
		if (year === "2019" && speed_incidents != 0){
			incidents = Number(incidents) + Number(speed_incidents);
			// we add 1 to days here, because the app assumes that the batch views were recalculated at the end of the prior day
			// and that all new incidents have been submitted through the form
			console.log("Integrating Speed Layer\n");
	    	console.log(weather);
	    	console.log("Incident Sum:" + incidents);
	    	console.log("Days sum:" + (Number(days)+1));
			return (incidents / (Number(days) + 1)).toFixed(1); 
		} else {
			// no days with the given weather type have occurred
			if(days == 0){
				return " N/A ";
			} else {
				return (incidents / days ).toFixed(1); 
			}	
		}  
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
});


// Third View
app.get('/arrestperday.html',function (req, res) {
    const year = req.query['year'];
    console.log("Extracting arrest rate data for year: ")
    console.log(year);
    const get = new hbase.Get(year);
    client.get("zepelin_hbase_crimes_by_day", get, function(err, row) {
	assert.ok(!err, "get returned an error: #{err}");
	if(!row){
            res.send("<html><body>No such year in data</body></html>");
            return;
        }

    const get2 = new hbase.Get(year);
	client.get("zepelin_hbase_crimes_by_day_speed", get2, function(err2, row2) {
		assert.ok(!err2, "get returned an error: #{err}");

	function daily_incidents(weather) {
	    var incidents = row.cols["year:" + weather + "incidents"].value;
	    var arrests = row.cols["year:" + weather + "arrests"].value;
	    var total_incidents = Number(incidents);
	    if (year === "2019"){
		    var speed_incidents = Number(BigIntBuffer.toBigIntBE(row2.cols["year:" + weather + "incidents"].value));
		    var speed_arrests= Number(BigIntBuffer.toBigIntBE(row2.cols["year:" + weather + "arrests"].value));
		    total_incidents = total_incidents + Number(speed_incidents);
		}
	    if (year === "2019" && total_incidents != 0){
	    	var total_arrests = Number(arrests) + Number(speed_arrests);
	    	console.log("Integrating Speed Layer\n");
	    	console.log(weather);
	    	console.log("Arrest Sum:" + total_arrests);
	    	console.log("Incident Sum:" + total_incidents);
	    	return (100 *  total_arrests / total_incidents).toFixed(1);
	    } else {
	    	if(incidents == 0){
				return " N/A ";
	    	}
	    	return (100 * arrests / incidents).toFixed(1);
	    }    
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
});
	


/* Send simulated weather to kafka */
var kafka = require('kafka-node');
var Producer = kafka.Producer;
var KeyedMessage = kafka.KeyedMessage;
var kafkaClient = new kafka.KafkaClient({kafkaHost: 'mpcs53014c10-m-6-20191016152730.us-central1-a.c.mpcs53014-2019.internal:6667'});
var kafkaProducer = new Producer(kafkaClient);


app.get('/submit.html',function (req, res) {
    var wind = req.query['wind'];
    var temp = req.query['temp'];
    var precip = req.query['precipitation'];
    var arrest = req.query['arrest'];
    var combo = wind + temp + precip;
    var report = {
	wind : wind,
	temp : temp,
	precip : precip,
	combo : combo,
	arrest : arrest,
    };

    kafkaProducer.send([{ topic: 'zepelin-incident', messages: JSON.stringify(report)}],
			   function (err, data) {
			       console.log(data);
			   });
    console.log(report);
    res.redirect('submit-data.html');
});
app.listen(port);
