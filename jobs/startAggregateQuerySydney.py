import boto3
import json
import os

session = boto3.Session(region_name="ap-southeast-2")
athena_client = session.client('athena')
glue_client = boto3.client('glue')

workflowName = 'Athena-cross-region-wf'
workflow = glue_client.get_workflow(Name=workflowName)
workflow_params = workflow['Workflow']['LastRun']['WorkflowRunProperties']
workflowRunId = workflow['Workflow']['LastRun']['WorkflowRunId']
DATABASE = workflow_params['SYDNEY_DATABASE_NAME']
# The S3 bucket\folder\ location where you would like query results saved.
OUTPUT = workflow_params['SYDNEY_OUTPUT_LOCATION']

response = athena_client.start_query_execution(
    QueryString='''INSERT INTO "nyctaxi-data-db-sydney"."sydney_yellow_aggregated" 
                    SELECT vendor_name, sum(trip_distance) as "total_distance" ,
                    substr("pickup_datetime",9,2) AS "day"
                    FROM "nyctaxi-data-db-sydney"."yellow" 
                    GROUP BY vendor_name, substr("pickup_datetime",9,2) LIMIT 100;''',
    QueryExecutionContext={
        'Database': DATABASE
    },
    ResultConfiguration={
        'OutputLocation': 's3://'+OUTPUT
    }
)

queryExecutionId = response['QueryExecutionId']

workflow_params['joinQueryExecutionIdSyd'] = queryExecutionId
glue_client.put_workflow_run_properties(Name=workflowName, RunId=workflowRunId, RunProperties=workflow_params)
workflow_params = glue_client.get_workflow_run_properties(Name=workflowName,
                                      RunId=workflowRunId)["RunProperties"]

print('Query execution id: ' + workflow_params['joinQueryExecutionIdSyd'])