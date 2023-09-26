# wbjt_assignment

Build and run
-------------
To build, goto wbjt directory in terminal and execute
1. docker build -t movies_dc:1.0 .

To run
1. docker run -v $(pwd):/usr/app/movies_app/out movies_dc:1.0 --apikey x-access-token=<replace_access_token>

Output will be seen in the console

Docker version used => Docker version 24.0.2-rd, build e63f5fa

Source files description
--------------------
1. main.py -> entry point to the app, controls and runs all the functionality
2. DataMgr.py -> Responsible for managing the data, paths and collectors, data polling and saving to disk
3. MovieDataCollector.py -> Interacts with the Movies APIs and fetches data from them, caches the session open a new session for every request
4. RequestAdapter.py -> Wrapper / Adapter for request, handles timeouts, can be extended to handle other exception

Config files
------------
app_conf.yaml -> Application config
log_conf.yaml -> Logging config

Key Design decisions
--------------------

1. Pandas vs PySpark:
   For any large dataset, would go with PySpark, as it can handle large dataset especially for those that do not fit in memory of single machine.
   Also, Spark / PySpark schema support seems to be lot better and I was inclined to change to Spark to solve the parquet problem, but did not due to time constraints
   Here since the dataset was very small, opted to use Pandas
   Also Pandas is more an analytical tool, I would not prefer it in prod for ETL or related stuff.
   
3. Concurrency: Running requests concurrently as the Apis are independent of each other.
   
5. Parquet: Storing data in parquet format with snappy compression for faster analytics

Issues to be addressed
-----------------------
1. Parquet generation is not proper, pandas not able to read from root folder, might be better to use spark for this, but is a overkill for the amount of data we need to analyze
2. Write data to temp/staging location before moving to data store.
3. Logs are not being stored in the container and not on a attached volume.
4. Defining schema and storing data in proper schema
5. Unit testing, could not get to it.


Assumptions
-----------
1. Movie title uniquelly identifies the movie and is same across both the providers.
2. History data is deleted and we always start on a clean slate.
   
   




