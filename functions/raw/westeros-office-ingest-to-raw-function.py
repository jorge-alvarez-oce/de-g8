from googleapiclient.discovery import build
from google.cloud import bigquery


def execDataflow(request):
    dataflow = build('dataflow', 'v1b3')

    projectId = "gcp-data-engineer-project-001"
    location = "us-east1"
    dataflowTemplate = "gs://westeros-df-config/raw/templates/westeros-jdbc-to-cloudstorage.json"

    parameters = {
        "driverClassName": "com.mysql.cj.jdbc.Driver",
        "connectionURL": "jdbc:mysql://34.125.166.191:3306/classicmodels",
        "username": "labfinalxs",
        "password": "clase@patch",
        "query": "select * from offices",
        "gcslake": "gs://westeros-g8-datalake/raw/offices.txt"
    }

    environment = {
        "numWorkers": 2,
        "tempLocation": "gs://westeros-df-config/job"
    }
    request = dataflow.projects().locations().templates().launch(
        projectId=projectId,
        location=location,
        gcsPath=dataflowTemplate,
        body={
            "jobName": "westeros-offices-ingest-to-raw-dataflow",
            "parameters": parameters,
            "environment": environment
        }
    )

    response = request.execute()
    return response
