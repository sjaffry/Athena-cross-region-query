import sys
import boto3
import time

session = boto3.Session(region_name='us-west-2') 
athena_client = session.client('athena')
glue_client = boto3.client('glue')

workflowName = 'Athena-cross-region-wf'
workflow = glue_client.get_workflow(Name=workflowName)
workflow_params = workflow['Workflow']['LastRun']['WorkflowRunProperties']
workflowRunId = workflow['Workflow']['LastRun']['WorkflowRunId']
queryExecId = workflow_params['joinQueryExecutionIdOregon']

queryStatus = athena_client.get_query_execution(QueryExecutionId=queryExecId)['QueryExecution']['Status']['State']

while (queryStatus != 'SUCCEEDED'):
    queryStatus = athena_client.get_query_execution(QueryExecutionId=queryExecId)['QueryExecution']['Status']['State']
    if (queryStatus == 'FAILED' or queryStatus == 'CANCELLED'):
        raise NameError('Query execution failed')
    time.sleep(20)

print ('Query execution status is: ' + queryStatus)