import os
from googleapiclient.discovery import build

projectId = "gcp-data-engineer-project-001"

def execDataflow(event, context):
    fullFileName = event['name']
    print(f"Name File: {fullFileName}")

    if "raw" in fullFileName and (".txt" in fullFileName or ".csv" in fullFileName):
        print("Running data flow pipeline")

        runDataflowPipeline(event)
    else:
        print("Skip pipe")

def runDataflowPipeline(event):

    location = "us-east1"
    dataflowTemplate = "gs://westeros-df-config/quality/templates/westeros-cloudstorage-to-bigquery.json"
    
    parameters = {
        "gcslake": buildGcsLakeName(event),
        "bigQueryLoadingTemporaryDirectory": "gs://westeros-g8-datalake/quality",
        "outputTable": getTableName(event)
    }

    environment = {
        "numWorkers": 2,
        "tempLocation": "gs://westeros-df-config/job"
    }

    dataflow = build('dataflow', 'v1b3')
    request = dataflow.projects().locations().templates().launch(
        projectId=projectId,
        location=location,
        gcsPath=dataflowTemplate,
        body={
            "jobName": "westeros-%s-ingest-from-raw-to-quality-dataflow"%(getEntity(event)),
            "parameters": parameters,
            "environment": environment
        }
    )

    response = request.execute()

def buildGcsLakeName(event):
    bucket = event['bucket']
    nameFile= event['name']
    print("Bucket ==> " + bucket)
    print("NameFile ==> " + nameFile)
    
    gcsLakeName ="gs://%s/%s"%(bucket,nameFile)
    print("GCS Lake Name ==> " + gcsLakeName)

    return gcsLakeName

def getTableName(event):
    return "%s.quality.%s"%(projectId,getEntity(event))

def getEntity(event):
    fullFileName = event['name']

    pathName, extension = os.path.splitext(fullFileName)
    fileName = pathName.split('/')
    return fileName[-1];