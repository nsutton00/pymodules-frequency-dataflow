#!/usr/bin/env python

"""
A workflow using BigQuery source and GCS sink.

Queries from the github public dataset `contents` to return files ending in .py

Parses the source code to produce a list of imported python modules 
Computes the frequency of each module

The results outputs to a CSV on Cloud Storage with the following schema:

-module_name:string
-frequency:int
"""

import apache_beam as beam
import sys
import argparse

def get_imports(contents):
   """
   Takes multi-line string as arguement
   Returns lines that begin with 'import' 
   """
   if contents is not None:
      contents = contents.lower()
      for line in contents.splitlines():
         if line.startswith('import'):
            yield line

def clean_import_lines(line):
   """
   Takes single-line string as arguement
   Cleans the line by:
   -removing the import text 
   -removing user defined aliases e.g. import time as t 
   -handling multiple imports on a single line e.g. import time as t, random as r 
   """
   line = line.replace('import', '')
   line = line.split(',')
   for module in line:
      module = module.split(' as ')
      yield [module[0].strip(), ''] #Count.PerKey expects 2 values, so passing dummy value

def run(known_args, pipeline_args):
   """Main entry point; defines and runs the pipeline"""
   p = beam.Pipeline(argv=pipeline_args)
   
   #Sample data set, useful for testing
   query_testing = 'SELECT content, sample_path FROM `bigquery-public-data.github_repos.sample_contents` WHERE REGEXP_CONTAINS(sample_path, \'(\\\.py)\') LIMIT 50'

   query = 'SELECT \
   content.id AS cid, \
   files.id AS fid, \
   content.content, \
   files.path, \
   FROM \
   `bigquery-public-data.github_repos.contents` AS content \
   JOIN \
   `bigquery-public-data.github_repos.files` AS files \
   ON \
   content.id = files.id \
   WHERE \
   REGEXP_CONTAINS(path, \'(\\\.py)\')'  

   (p
      
      | 'GetDataFromBQ' >> beam.io.Read(beam.io.BigQuerySource(query=query_testing, use_standard_sql=True))
      | 'GetImportLines' >> beam.FlatMap(lambda line: get_imports(line['content']))
      | 'CleanImportLines' >> beam.FlatMap(lambda line: clean_import_lines(line))
      | 'CountFrequency' >> beam.combiners.Count.PerKey()
      | 'FormatForCSV' >> beam.Map(lambda line: ','.join(map(str,line)))
      | 'Output' >> beam.io.WriteToText('gs://{0}/output'.format(known_args.bucket), file_name_suffix='.csv', header='module_name, frequency')
   )

   if known_args.runner == "DataFlowRunner":
      p.run()
   else:
      p.run().wait_until_finish()


if __name__ == '__main__':
   '''
   User specifies pipeline options at runtime

   Project and Bucket name are required
   Job_name and Runner are optional
   Staging and temp locations are created within the bucket provided
   '''
   parser = argparse.ArgumentParser()
   parser.add_argument(
      'project',
      help='GCP Project Id.')
   parser.add_argument(
      'bucket',
      help='Name of GCS bucket.')
   parser.add_argument(
      '--job_name',
      help='Job name as it will appear in logs. Defaults to beamjob.', default="mybeamjob")
   parser.add_argument(
      '--runner',
      help='The Beam runner. Defaults to Dataflow.', default="DataFlowRunner")
   parser.add_argument(
      '--region',
      help='The region to run the Dataflow job. Future releases will require this to be explicitly set. Defaults to us-central1', default="us-central1")
   known_args, pipeline_args = parser.parse_known_args()
   pipeline_args.extend([
      '--project={0}'.format(known_args.project),
      '--job_name={0}'.format(known_args.job_name),
      '--runner={0}'.format(known_args.runner),
      '--staging_location=gs://{0}/staging'.format(known_args.bucket),
      '--temp_location=gs://{0}/temp'.format(known_args.bucket),
      '--save_main_session'
   ])

   run(known_args, pipeline_args)

