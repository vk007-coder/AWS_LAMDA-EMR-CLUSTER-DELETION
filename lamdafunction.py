import boto3
import os
from botocore.exceptions import ClientError

def terminate_cluster(cluster_id, emr_client):
    """Terminate the specified EMR cluster."""
    try:
        # Terminate the EMR cluster
        response = emr_client.terminate_job_flows(JobFlowIds=[cluster_id])
        
        if response['ResponseMetadata']['HTTPStatusCode'] == 200:
            return {"status": "success", "message": f"EMR Cluster {cluster_id} termination initiated."}
        else:
            return {"status": "failed", "message": f"Failed to terminate EMR Cluster {cluster_id}."}
    
    except ClientError as e:
        return {"status": "failed", "message": f"Error terminating cluster: {e.response['Error']['Message']}"}

def delete_logs(bucket_name, prefix, s3_client):
    """Delete logs from the specified S3 bucket and prefix."""
    try:
        # List the objects in the specified path
        response = s3_client.list_objects_v2(Bucket=bucket_name, Prefix=prefix)
        
        if 'Contents' in response:
            # Prepare the list of objects to delete
            delete_objects = {'Objects': [{'Key': obj['Key']} for obj in response['Contents']]}

            # Delete the objects
            s3_client.delete_objects(Bucket=bucket_name, Delete=delete_objects)
            return {"status": "success", "message": f"Logs from {prefix} deleted successfully."}
        else:
            return {"status": "info", "message": "No logs found."}
    
    except ClientError as e:
        return {"status": "failed", "message": f"Error deleting logs: {e.response['Error']['Message']}"}

def get_cluster_details(cluster_id, emr_client):
    """Fetch details of the specified EMR cluster."""
    try:
        response = emr_client.describe_cluster(ClusterId=cluster_id)
        cluster_status = response['Cluster']['Status']['State']
        return {"status": cluster_status, "message": f"Cluster {cluster_id} is {cluster_status}."}
    
    except ClientError as e:
        return {"status": "failed", "message": f"Error fetching details for cluster {cluster_id}: {e.response['Error']['Message']}"}

def lambda_handler(event, context):
    """AWS Lambda function to terminate EMR cluster and delete associated logs from S3."""
    
    # Fetch cluster IDs from event, default to empty list
    cluster_ids = event.get("ClusterIds", [])
    
    if not cluster_ids:
        return {"status": "failed", "message": "ClusterIds are required."}
    
    # Get the region from the environment variable
    region = os.getenv("EMR_REGION", "us-east-1")  # Default to us-east-1 if not set
    
    # Create AWS clients using the dynamically fetched region
    emr_client = boto3.client('emr', region_name=region)
    s3_client = boto3.client('s3', region_name=region)
    
    results = []
    
    for cluster_id in cluster_ids:
        result = {}
        
        # Step 1: Get cluster details (check status)
        cluster_details = get_cluster_details(cluster_id, emr_client)
        
        terminate_result = {}  # Initialize terminate_result to ensure it's always defined
        
        if cluster_details['status'] == 'TERMINATED':
            result[cluster_id] = {"status": "info", "message": f"Cluster {cluster_id} is already terminated."}
            # Step 2: Attempt to delete logs for terminated clusters
            log_bucket = os.getenv("EMR_LOG_BUCKET", "aws-logs-108782056827-us-east-1")  # Default bucket name
            log_prefix = f"elasticmapreduce/{cluster_id}/"
            delete_logs_result = delete_logs(log_bucket, log_prefix, s3_client)
            result[cluster_id]['delete_logs_status'] = delete_logs_result
        
        elif cluster_details['status'] == 'TERMINATED_WITH_ERRORS':
            result[cluster_id] = {"status": "error", "message": f"Cluster {cluster_id} is terminated with errors."}
            # Step 3: Attempt to delete logs for clusters terminated with errors
            log_bucket = os.getenv("EMR_LOG_BUCKET", "aws-logs-108782056827-us-east-1")  # Default bucket name
            log_prefix = f"elasticmapreduce/{cluster_id}/"
            delete_logs_result = delete_logs(log_bucket, log_prefix, s3_client)
            result[cluster_id]['delete_logs_status'] = delete_logs_result
        
        else:
            # Step 2: Terminate the EMR cluster if not already terminated
            terminate_result = terminate_cluster(cluster_id, emr_client)
            result[cluster_id] = terminate_result
            # Step 3: Delete logs from S3 if cluster is terminated or in error state
            if terminate_result.get("status") == "success" or cluster_details['status'] == 'TERMINATED_WITH_ERRORS':
                log_bucket = os.getenv("EMR_LOG_BUCKET", "aws-logs-108782056827-us-east-1")  # Default bucket name
                log_prefix = f"elasticmapreduce/{cluster_id}/"
                delete_logs_result = delete_logs(log_bucket, log_prefix, s3_client)
                result[cluster_id]['delete_logs_status'] = delete_logs_result
        
        # Add result for the current cluster
        results.append(result)
    
    return {"results": results}
