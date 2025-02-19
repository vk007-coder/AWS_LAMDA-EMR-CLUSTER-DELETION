import boto3
import os
import re
from botocore.exceptions import ClientError
from concurrent.futures import ThreadPoolExecutor

def get_cluster_region(cluster_id):
    """Fetch the region of an EMR cluster dynamically."""
    regions = os.environ.get("EMR_REGIONS", "us-east-1").split(",")  # Using os.environ.get()

    for region in regions:
        # Clean the region name by removing any non-alphanumeric characters (like tabs, spaces, etc.)
        cleaned_region = re.sub(r'\s+', '', region)  # This removes any whitespace characters

        try:
            emr_client = boto3.client("emr", region_name=cleaned_region)
            if emr_client.describe_cluster(ClusterId=cluster_id).get("Cluster"):
                return cleaned_region
        except ClientError as e:
            if e.response["Error"]["Code"] == "ClusterNotFoundException":
                continue  # Try next region
    return None

def terminate_cluster(cluster_id, region):
    """Terminate a cluster."""
    emr_client = boto3.client("emr", region_name=region)
    try:
        cluster_state = emr_client.describe_cluster(ClusterId=cluster_id)["Cluster"]["Status"]["State"]
        if cluster_state == "TERMINATED":
            return {"status": "info", "cluster_id": cluster_id, "message": f"Cluster {cluster_id} is already terminated in {region}."}
        else:
            emr_client.terminate_job_flows(JobFlowIds=[cluster_id])
            return {"status": "success", "cluster_id": cluster_id, "message": f"Cluster {cluster_id} termination initiated in {region}"}
    except ClientError as e:
        return {"status": "failed", "cluster_id": cluster_id, "error": e.response["Error"]["Message"]}

def lambda_handler(event, context):
    """AWS Lambda function to find and terminate EMR clusters."""
    cluster_ids = event.get("ClusterIds", [])
    if not cluster_ids:
        return {"status": "failed", "message": "ClusterIds not provided"}

    results = []
    with ThreadPoolExecutor() as executor:
        # Get the region for each cluster in parallel
        futures = []
        for cluster_id in cluster_ids:
            region = get_cluster_region(cluster_id)
            if region:
                futures.append(executor.submit(terminate_cluster, cluster_id, region))
            else:
                results.append({"status": "failed", "cluster_id": cluster_id, "message": f"Cluster {cluster_id} not found in configured regions"})

        # Collect the results of all futures
        for future in futures:
            results.append(future.result())

    return {"results": results}
