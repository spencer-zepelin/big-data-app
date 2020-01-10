NOTE: THIS README IS FROM THE FIRST SUBMISSION OF THE PROJECT
IT HAS BEEN INCLUDED HERE FOR REFERENCE
PLEASE SEE READMEv2.txt FOR THE CURRENT SUBMISSION

Big Data Application Architecture
Professor Spertus
MPCS 53014

Final Project
Dog Day Afternoon: Relating Weather Data to Chicago Crime

Spencer Zepelin

December 4, 2019

------------------

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
old build and commence with a new one are in "/home/zepelin
/zepelin_final".

For convenience, all that is necessary to build the data backend for the 
application is running the script "run.sh" in the directory "/home/zepelin
/zepelin_final". This calls subordinate commands to build the entire 
backend. These other scripts, queries and commands are organized into the 
batch and serving layer in the corresponding directories.

run.sh first begins by ingesting the datasets into HDFS. Note: Two lines 
have been commented out of run.sh that, if uncommented, will allow you to 
reingest all data from both sources.

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


Running the Application
--

The application has been left running, but it can be restarted with the 
following procedure.

After logging on to the webserver on the cluster, navigate to the 
directory:
	/home/zepelin/zepelin_app
This directory contains the various files that make up  (included here in 
the deliverables in a directory also named zepelin_app for ease of 
reference). In this location, enter the following command:
	node app.js
The application is now running.

In any web browser navigate to any of the following three pages:
	http://34.66.189.234:3525/all-combos.html
	http://34.66.189.234:3525/crime-rate.html
	http://34.66.189.234:3525/arrest-rate.html
You will be greeted by a form asking you to enter a year from 2001 to 
2019.

Upon submitting a year, you will receive the precomputed data for that 
view from HBase.