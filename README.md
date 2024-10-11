This Python script is designed to query Kafka topics for messages within specified timestamps, with an optional filter to search messages containing a specific input string.


# **Pre-requisites:**



1. Python version 3.8.10


# **Steps:**


## Step 1: Optional: Create a Virtual environment 


```
python3 -m venv env

source env/bin/activate
```



## Step 2: Install Dependencies 


```
pip install -r requirements.txt
```



## Step 3 : configure *client.properties* file 

Create and configure the *client.properties* file with the following information:


```
[DEFAULT]
# Required connection configs for Kafka producer, consumer, and admin
bootstrap.servers=<bootstrape_server:port>
security.protocol=SASL_SSL
sasl.mechanisms=PLAIN
sasl.username=<apikey>
sasl.password=<apisecret>
session.timeout.ms=45000
```


Make sure to replace the placeholder values (&lt;bootstrap_server:port>, &lt;apikey>, and &lt;apisecret>) with your actual Kafka credentials.
**Note:** The API key and secret provided must have permission to read all relevant topics and the group ID (which can be provided in the client.properties file). If no group ID is provided, then read permission must be granted to 'foo,' which is the default group ID.  


## Step 4: Run the *query-topic.py* Script with Input Parameters

 Here is a list of available options:


<table>
  <tr>
   <td><strong>Options</strong>
   </td>
   <td><strong>Description </strong>
   </td>
   <td><strong>Required/Optional</strong>
   </td>
  </tr>
  <tr>
   <td><code>--properties_file</code>
   </td>
   <td>path of properties file to be used 
   </td>
   <td>Required
   </td>
  </tr>
  <tr>
   <td><code>--start_time</code>
   </td>
   <td>Start time in 'YYYY-MM-DD HH:MM:SS' IST format
   </td>
   <td>Required
   </td>
  </tr>
  <tr>
   <td><code>--end_time</code>
   </td>
   <td>End time in 'YYYY-MM-DD HH:MM:SS' IST format
   </td>
   <td>Required
   </td>
  </tr>
  <tr>
   <td><code>--topic</code>
   </td>
   <td>Topic name 
   </td>
   <td>Required
   </td>
  </tr>
  <tr>
   <td><code>--string</code>
   </td>
   <td>String to search for in messages
   </td>
   <td>Optional
   </td>
  </tr>
  <tr>
   <td><code>--csv</code>
   </td>
   <td>CSV file name to write messages to
   </td>
   <td>Optional
   </td>
  </tr>
</table>



# **Usage:**


## Display help

To display help for usage instructions, run the following command:


```
python3 query-topic.py --help
```



![Screenshot from 2024-10-10 15-33-35.png](</img/Screenshot from 2024-10-10 15-33-35.png>)



## Query All Messages Between Start and End Times:


```
python3 query-topic.py --start_time '2024-10-10 11:13:59' --end_time '2024-10-10 11:14:10' --properties_file client.properties --topic demotopic
```


This command retrieves all messages between the specified start time (2024-10-10 11:13:59) and end time (2024-10-10 11:14:10).

**Note:** The start and end times must be in single quotes and follow the 'YYYY-MM-DD HH:MM:SS' format.



![Screenshot from 2024-10-10 15-09-25.png](<img/Screenshot from 2024-10-10 15-09-25.png>)



## Query Messages with a Specific String Between Start and End Times:


```
python3 query-topic.py --start_time '2024-10-10 11:10:00' --end_time '2024-10-10 11:14:10' --properties_file client.properties --topic demotopic --string '"city":"City_3"'
```


This command retrieves all messages between the start time and end time that contain the string "city":"City_3".

**Note:** The string filter must be enclosed in single quotes.



![Screenshot from 2024-10-10 15-08-17.png](<img/Screenshot from 2024-10-10 15-08-17.png>)



## Save Output to a CSV File: 


```
python3 query-topic.py --start_time '2024-10-10 11:10:00' --end_time '2024-10-10 11:14:10' --properties_file client.properties --topic demotopic --string '"city":"City_3"' --csv topic-query.csv
```


This command saves the output to the specified topic-query.csv file.



![Screenshot from 2024-10-10 15-06-54.png](<img/Screenshot from 2024-10-10 15-06-54.png>)



![Screenshot from 2024-10-10 15-07-09.png](<img/Screenshot from 2024-10-10 15-07-09.png>)



# Note: 



* This script currently supports messages in Plain JSON format only without using schema registary