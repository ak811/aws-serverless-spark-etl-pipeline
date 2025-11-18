import boto3

# Replace with the name of your Glue job
GLUE_JOB_NAME = "process_reviews_job"

def lambda_handler(event, context):
    """
    This Lambda function is triggered by an S3 event and starts a Glue ETL job.
    """
    glue_client = boto3.client('glue')

    try:
        print(f"Starting AWS Glue job: {GLUE_JOB_NAME}")
        response = glue_client.start_job_run(JobName=GLUE_JOB_NAME)
        print(f"Successfully started job run. Run ID: {response['JobRunId']}")
        return {
            'statusCode': 200,
            'body': f"Glue job {GLUE_JOB_NAME} started successfully."
        }
    except Exception as e:
        print(f"Error starting Glue job: {e}")
        raise e
