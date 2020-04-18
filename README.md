# Popular python modules from GitHub data

This is a sample project created to practice Apache Beam running on Dataflow. It uses the GitHub public dataset to calculate the popularity of python modules by counting their imports into files on GitHub. 

## Description of PTransforms 

### GetDataFromBQ
Queries the GitHub public dataset to return the contents of files ending in .py

```
SELECT 
   content.id AS cid, 
   files.id AS fid, 
   content.content, 
   files.path, 
   FROM 
   `bigquery-public-data.github_repos.contents` AS content 
   JOIN 
   `bigquery-public-data.github_repos.files` AS files 
   ON 
   content.id = files.id 
   WHERE 
   REGEXP_CONTAINS(path, '(\\\.py)')  
```

Note: a sample dataset query is provided in the source, this represents a subset of all avalible data and is useful for testing and running locally.

### GetImportLines
For each line in each python file only return lines beginning with 'import'. 

### CleanImportLines
Import lines may not all adhere to a ```import <module>``` format so some cleaning is required.
This PTransform performs the following:
* removes the ```import``` text 
* handles user defined aliases e.g. ```import time as t```
* handles multiple imports on a single line e.g. ```import time as t, random as r```

### CountFrequency

Uses ```beam.Count.PerKey()``` to calculate the number of times each module appears in our PCollection. 

### FormatForCSV
Prior to this PTransform, data is stored as a tuple ```('name','frequency')```.

We're outputting to a CSV file, so we will need to perform some formatting.

### Output

The results are stored as CSV in the GCS bucket specified at runtime with the following schema:
```
module_name:string
frequency:int
```

## Deployment

To run this workflow, git clone the repo. Navigate to the directory and run using the below.
```
python pymodule_frequency.py \ 
<GCP project> \  
<GCS bucket where output will go> \ 
--job_name=<The name of your beam job> [OPTIONAL: Defaults to myBeamJob]\
--runner=DataflowRunner [OPTIONAL: Defaults to DataFlowRunner] \
```
## Results and learnings
Dataflow proccessed 2.5 TB of data and took just under 8 minutes to complete.

At the time of running, these are the top 5 most frequently imported python modules on Github.

| Module Name | Frequency |
|-------------|-----------|
| os          | 822885    |
| sys         | 730218    |
| re          | 322168    |
| time        | 289101    |
| logging     | 279431    |
