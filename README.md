# Introduction
Nasa Web Access Log Analyzer Application

## Objective
- Fetch top N visitor
- Fetch top N urls

### Code walkthrough
Input Download URL - ftp://ita.ee.lbl.gov/traces/NASA_access_log_Jul95.gz	
1. src/main/scala
	* *gov.nasa.loganalyzer.NasaWebAccessStats.scala* - This is main entry class. SparkSession object is build here.
	* *gov.nasa.loganalyzer.NasaWebAccessStatsProcessor.scala* - The spark Dataset processing logic is present here.
2. src/main/resources
	* *accessloganalyzer.conf* - all configurations are maintained in a Typesafe config file.
3. src/test/scala
	* *gov.nasa.test.loganalyzer.NasaWebAccessStatsProcessorTest.scala* - This is the Unit test class for NasaWebAccessStatsProcessor.
4. *ScalaSyleGuide.xml* - This is the formatter configuration file used to format the code.

### Configurations
Configuration file name- *accessloganalyzer.conf*

Properties set in configuration file-
- **valueOfN**- This property is to set the value of N in the topNVisitors and topNUrls. *REQUIRED INT value*
- **writeCurruptLogEnteries**- Some of the log lines are corrupt. If writeCurruptLogEnteries is set to TRUE, then corrupt lines will be writen to filesystem, else not. *BOOLEAN value*
- **filterResponseCodes**- When evaluating topNUrls, this property is used to filter the log lines with undesired response code. Eg- when set to value ["304","400"], all log lines with response code 304 and 400 will be filtered. *LIST value*
- **downloadFileLoc**- This is the ftp location from where the input gz file will be downloaded. If set to blank, then file-download will not happen, assuming that the file is already on local filesystem. *STRING value*
- **gzFSLocation**- This is the local filesystem path where gz file is downloaded. If the property "downloadFileLoc" is not set, then the application assumes that the gz file is present at this location. *REQUIRED STRING value*
	
Incase required configuration properties are not set, then the application exits with code 0.

### Assumptions
1. Data is structured in the following format- 
	* `<visitor> - - [<date> <timezone>] "<method> <url> <protocol>" <resonseCode> <unknownvariable>`
  	  `E.g.- unicomp6.unicomp.net - - [01/Jul/1995:00:00:14 -0400] "GET /shuttle/countdown/count.gif HTTP/1.0" 200 40310`
  	  String split functions have been used to derive date and other attributes based on the assumption that the data log line will follow this format.

2. When log-lines are tokenized using tokenizer **SPACE** (" "), there are entries with-
	* *10 tokens* - log-lines with 10 tokens have protocol value set as HTTP/1.0
	* *9 tokens* - log-lines with 9 tokens don't have any protocol value set.
	* *8 or less tokens* - these are considered as corrupt lines as it has incomplete information, such as missing method(GET,POST,HEAD etc) and/or missing other information.
	
3. To evaluate topNurls, the application gives two options-
	* filterResponseCodes set with a value - when set with a list of responseCodes, the log-lines with the specified response codes will be filtered out. Eg. a 404 response request can be filtered out when processing top N urls.
	* filterResponseCodes set BLANK - when set to blank, all log-lines will be considered valid to evaluate the top N urls. Only lines missed will be corrupt lines identified as tokens < 9.
   
4. To evaluate topNvisitors, no special logic has been added to filter log-lines based on HTTP method. The current implementation considers GET, POST, HEAD etc methods as the visitors. 
   
### Software versions
	- Scala version- 2.11.8
	- Spark version- 
	- SBT version- 1.0.1
	- IDE- Eclipse Scale IDE
	
### Steps to compile
1. Go to the project root directory, where build.sbt is present
2. Run cmd- `sbt clean assembly`. By default this will not run test.Refer the below section to see how to run tests.
3. The jar is generated in the target directory. Check jar full path in the console-log.


### Steps to run unit test
1. Go to the project root directory, where build.sbt is present
2. Run cmd- `sbt test`.
3. The test report is generated in the console-log.

### Steps to run the application on the local machine
#### System Setup
mkdir <app_base_path>/conf

mkdir <app_base_path>/input

mkdir <app_base_path>/input/jar

mkdir <app_base_path>/output
	
	
Copy accessloganalyzer.conf to <app_base_path>/conf

Copy jar to <app_base_path>/input/jar

Update accessloganalyzer.conf environment specific properties.

Run application

#### Command to execute	
```
bin/spark-submit \
--class gov.nasa.loganalyzer.NasaWebAccessStats \
--master local[4] \
<full-path-to-jar's-dir>/NasaWebAccessStats.jar <full-path-of-accessloganalyzer.conf> <full-path-of-output-dir-with-trailing-slash>
```

**Note**- If configuration property `downloadFileLoc` is set as "", in the config file, then the download of file will not happen. Instead the value of configuration property `gzFSLocation` will be used as the input file location for spark.

### Running it in docker
This code is packaged in 2 types of docker files
1. Packaged with data and artifacts. Packaed with the jar, configuration file and the data file already downloaded.
2. Mounted with data and artifacts : Packaged with nothing. the jar, configuration file and the file needs to be made available in the attched volume.

#### Packaged with data and artifacts
To run this docker file
- Download it from https://1drv.ms/u/s!AqoZASkgVNRcaU8YLBCyu1PUDqs?e=Ar7G3o
- Run `gzip -d spark-docker.tar.gz`
- Run `docker load --input spark-docker.tar`
- Run `docker run -p 4040:4040 -v <your_host_path>/spark-data:/opt/spark-data spark-docker:2.3.3`
- The output will be written to <your_host_path>/spark-data/NasaWebAccessStats/ouput
	
#### Mounted with data and artifacts
To run this docker file
- Download it from https://1drv.ms/u/s!AqoZASkgVNRcajWQ7OgcNvmK0DQ?e=sTY1IY
- Run `gzip -d spark-docker-v2.tar.gz`
- Run `docker load --input spark-docker-v2.tar`
- Run `docker run -p 4040:4040 -v <your_host_path>/spark-data:/opt/spark-data -v <your_host_path>/spark-apps:/opt/spark-apps spark-docker-v2:2.3.3`
- The output will be written to <your_host_path>/spark-data/NasaWebAccessStats/ouput
- The jar needs to be copied to <your_host_path>/spark-apps/NasaWebAccessStats/jar
- The configuration file needs to be copied to <your_host_path>/spark-apps/NasaWebAccessStats/conf
- *The input file needs to be copied to <your_host_path>/spark-data/NasaWebAccessStats/input*
	
**Limitations with this Docker setup**
Running with docker desktop on mac, doesn't give the ability to access the internet from the container, out of the box. As a result the file has to be made available. If you run the code on your local, then the download works just fine.



