Big Data Application Architecture
Professor Spertus
MPCS 53014 2

Final Project, version 2
Dog Day Afternoon: Relating Weather Data to Chicago Crime

Spencer Zepelin

December 12, 2019

------------------



Modifications from Version 1
--

For version two, I constructed a speed layer for my application. The
form "/submit-data.html" sends new incident data into a Kafka queue
with the topic "zepelin-incident". A Spark job is then listening to 
ingest data from that queue and increment the appropriate tables in 
HBase.

Each view in the web app itself now queries two tables from HBase 
with nested callbacks: a precomputed batch view (as in v1) and an 
incremental speed layer counter. It merges these data together before
returning the result to the user.

In constructing the speed layer, I envisioned the following theoretical
architecture: 

Batch views are recomputed nightly and Chicago law enforcement
use the submission form to submit new incidents from the current day.
The speed layer tables that count these new submissions are set back
to zero nightly when the batch views are recomputed since the canonical version
of the prior day's data (including the objective--as opposed to subjective--weather 
readings) are now integrated with the batch views.

For this reason, new submissions only submit for the current year. Though this is
hardcoded as "2019" for convenience, modest changes to the application could have
it extract the year from the current datetime to obviate the need to update the 
application every year.

New work for version 2 can be found in the following files in the directory
"/home/zepelin/zepelin_final":

	run.sh --> Construction of speed layer tables and Spark job submission
	zepelin_app/app2.js --> Nested callbacks for merging batch and speed layer tables and
							data submission to Kafka queue
	zepelin_app/public/submit-data.html --> Form for new incident reporting
	zepelin_speed/* --> Three files constructing the new HBase tables, initializing 
						their values, and the uber jar for the Spark job
	zepelin_speed/speed_layer/* --> The cleaned Eclipse Maven project for the Spark job. Of 
									particular note are the Scala files in the subdirectory
									src/main/scala:
						- IncidentReport.scala --> which describes the data class
						- StreamIncident.scala --> which ingests from the queue and 
												   increments the HBase speed layer tables



Rationale and Findings
--

Are crime rates correlated to weather patterns? This project combines 
weather data from the NOAA and crime data from the Chicago Open Data 
Portal to explore that question. 

Weather data focuses on three phenomena: average windspeed, temperature, 
and precipitation. I performed exploratory data analysis to evaluate the 
quality of the weather data and determine the appropriate ways to handle 
it. This analysis can be found in the Jupyter Notebook 
"Weather_EDA.ipynb". Based on my analysis, I elected to bin weather into 
categories: low, moderate, and high for windspeed and 
precipitation--where low represents the lower quartile, high the upper 
quartile, and moderate the middle 50%--and low and high for 
precipitation--where high represents the upper quartile and low the 
remainder.

The first view "all-combos.html" looks at the 18 combinations of these 
categorical variables and counts the total number of reported incidents 
of each combination for a given year. 

Still, these counts are likely strongly correlated with the number of 
days of a given weather combination, so the second 
view--"crime-rate.html"--aims to report on the number of incidents per 
day of a given weather type. Additionally, this counts each weather bin 
on its own rather than in combination. As such, each crime will be 
counted once across every phenomenon. 

Here, we see a fairly stark pattern begin to observe in temperature. The 
crime rate per day rises markedly with temperature.

As a final view, "arrest-rate.html" looks at the percentage of incidents 
for which there is an arrest based on the same weather types as 
"crime-rate.html". Most interestingly, we see an inverse relationship in 
the arrest rate with temperature, with the arrest rate decreasing with 
rising temperatures. Speculatively, we might consider that law 
enforcement is able to pursue a higher percentage of incidents when there 
are fewer incidents in total, which would seem to provide support for 
expansion of resources for law enforcement if the public is willing to 
stomach the cost.



Building the Pipeline
--

The application is currently built, but all files needed to remove the 
old build and commence with a new one are in "/home/zepelin/zepelin_final".

For convenience, all that is necessary to build the data backend for the 
application is running the script "run.sh" in the directory "/home/zepelin
/zepelin_final". This calls subordinate commands to build the entire 
backend. These other scripts, queries and commands are organized into the 
batch, serving, and speed layers in the corresponding directories.

run.sh first begins by ingesting the datasets into HDFS. Note: Four lines 
have been commented out of run.sh that, if uncommented, will allow you to 
reingest all data from both sources. As is, data is presently in HDFS due
to storage constraints on the cluster.

Next, these datasets are loaded into Hive and then serialized into ORC 
tables. In transforming the tables, the date string fields are 
manipulated into integer fields representing year, day, and month. Rows 
from the crime dataset missing key fields are summarily dropped. Missing 
weather data, however, is interpolated with the median value for that 
measurement determined during EDA. Since there is only one row of weather 
data for each day, dropping entire days would much more radically 
influence our outcomes.

These datasets are then joined in two separate manners. One dataset left 
joins weather onto crime data such that the row-level entity is a crime 
incident. The other aggregates crimes by day and left joins them onto to 
the weather table such that a row represents a single day, its weather, 
and the number of crimes and arrests. 

The joined tables are then manipulated into new views that are 
subsequently loaded into HBase tables for use by the application.

Incremental speed layer tables are added to HBase. Each of these
tables is partnered with a batch view HBase table. Finally, the script
submits a Spark job. This job ingests new incidents reported in the queue
and increments speed layer HBase tables. 

At the conclusion of the script, the terminal will show two second batches 
for the Spark job and will print to screen any new data the job ingests.



Running the Application
--

The application has been left running, but it can be restarted with the 
following procedure.

After logging on to the webserver on the cluster, navigate to the 
directory:
	/home/zepelin/zepelin_app
This directory contains the various files that make up the application 
(included here in the deliverables in a directory also named zepelin_app 
for ease of reference). In this location, enter the following command:

	node app2.js

NOTE: Be sure to run app2.js not app.js, the latter of which is the version1 
application, which has been retained for reference. Of note also, the app is 
now running on port 3526 though on the initial submission it ran on 3525 as 
one of my colleagues is likely using that port.

The application is now running.

In any web browser navigate to any of the following three pages:
	http://34.66.189.234:3526/all-combos.html
	http://34.66.189.234:3526/crime-rate.html
	http://34.66.189.234:3526/arrest-rate.html
You will be greeted by a form asking you to enter a year from 2001 to 
2019.

Upon submitting a year, you will receive the precomputed data for that 
view from HBase. If you request data from 2019 for any of the three views,
the precomputed data will be merged with the speed layer data.

In order to submit new data, navigate to the following page:

	http://34.66.189.234:3526/submit-data.html

Select a set of weather conditions for a new incident and weather or not an
arrest has been made for that incident.


Validating the Speed Layer
--

In order to validate the successful functioning of the speed layer, I recommend
having the web pages open, the terminal with the listening Spark job, an HBase Shell,
and a listening web app that is not running in the background.

You will use the following four HBase commands:

	- get 'zepelin_hbase_yearly_combos', '2019'
	- get 'zepelin_hbase_yearly_combos_speed', '2019'
	- get 'zepelin_hbase_crimes_by_day', '2019'
	- get 'zepelin_hbase_crimes_by_day_speed', '2019'

Both speed tables should show hexadecimal counter values while their partner tables show
the aggregate values. 

Submit a new incident with the "/submit-data.html" form. On the webserver, you should see the
data coming in as well as its position in the queue. Pop over to the Spark job to see 
verification that the data has been ingested into the HBase tables. Query the speed HBase 
tables with the above commands to see that the counts have been incremented.

It is easiest to validate that the app is successfully integrating the speed layer data
by querying 2019 data on the "/all-combos.html" view. "Total Incidents" should now
represent the sum of totalincidents from 'zepelin_hbase_yearly_combos' and its speed layer
partner as should any weather combination that has also been incremented. We can easily 
increment this by going back and forth between submitting a new incident and then requerying 
the data for 2019.

Because it is late in the year and there is a great deal of data, it is difficult to notice 
the difference made by the speed layer in either the "/crime-rate.html" or 
"/arrest-rate.html" view. As such, I coded the web app to print to screen the aggregate data 
it uses to calculate those rates when it uses the speed layer. If the year is not 2019 or a 
speed layer counter is at zero, it forgoes printing these data to screen. Using the two 
"hbase_crimes_by_day" queries, we can validate both that new incidents have been
successfully ingested and that the web app is making its calculations using the sum of the
batch layer and speed layer data.


