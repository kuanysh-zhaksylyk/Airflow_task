# Airflow_task
## Review
### We need to create a DAG to process data and load it into MongoDB.
### The DAG should consist of several tasks:
----------------------------------
![The schema of architecture](/airflow_stack.drawio.png)

### Task 1: Sensor
* Create a Sensor that will respond to the appearance of our data file in a folder.

* After the Sensor is triggered, the following tasks should start, which will be responsible for processing the data. All data processing tasks should be combined into a separate TaskGroup. Each task must be responsible for a separate functionality:
---------------------
### Task 2: Data cleaning and pre-processing;
* Replace all "null" values with "-";
* Sort data by created_date;
* Remove all unnecessary characters from the content column (for example, emoticons, etc.), leave only text and punctuation marks.
----------------------
### Task 3: Load Data
* The last step is to create a task to load the processed data into MongoDB. To do this, you need to configure MongoDB and Connections configuration in Airflow locally.
---------------------
### Task 4: Queries

* Once you have transferred all the processed data to MongoDB, run the following queries (directly in MongoDB, for example, the Aggregations tab in MongoDB Compass):

1. Top 5 famous comments;

2. All entries where the length of the “content” field is less than 5 characters;

3. Average rating for each day (the result should be in the form of timestamp type).
--------
## Dataset

The data set consists of .csv extension files in folder named "dataset".
* dataset/tiktok_google_play_reviews.csv
--------
## Technologies:
In this task, we need to deploy and configure a working environment with all the necessary tools:

1. Airflow;

2. Python;

3. Pandas;

4. MongoDB.

--------
## Other:
All queries are stored in mongo_query


