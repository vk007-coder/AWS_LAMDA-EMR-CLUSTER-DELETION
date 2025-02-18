import boto3
import os

def get_cluster_region(cluster_id):
    """Fetch the region of an EMR cluster dynamically."""
    regions = os.getenv("EMR_REGIONS", "us-east-1").split(",")  # Get regions from env variable

    for region in regions:
        region = region.strip()  # Strip any leading/trailing whitespace or tab characters
        try:
            emr_client = boto3.client("emr", region_name=region)
            response = emr_client.describe_cluster(ClusterId=cluster_id)
            if response.get("Cluster"):
                return region  # Return the detected region
        except emr_client.exceptions.ClusterNotFoundException:
            continue  # Try the next region
    
    return None  # Return None if not found

def lambda_handler(event, context):
    cluster_id = event.get("ClusterId")
    if not cluster_id:
        return {"status": "failed", "message": "ClusterId not provided"}

    # Find the EMR cluster's region dynamically
    region = get_cluster_region(cluster_id)
    
    if not region:
        return {"status": "failed", "message": f"Cluster {cluster_id} not found in configured regions"}

    # Now check the status of the EMR cluster before terminating
    emr_client = boto3.client("emr", region_name=region)
    
    try:
        # Describe the cluster to get the status
        response = emr_client.describe_cluster(ClusterId=cluster_id)
        cluster_state = response["Cluster"]["Status"]["State"]

        if cluster_state == "TERMINATED":
            # Log that the cluster is already terminated
            print(f"Cluster {cluster_id} is already terminated.")
            return {
                "status": "info",
                "message": f"EMR Cluster {cluster_id} is already terminated in {region}.",
            }

        # If the cluster is not terminated, proceed to terminate it
        terminate_response = emr_client.terminate_job_flows(JobFlowIds=[cluster_id])
        return {
            "status": "success",
            "message": f"EMR Cluster {cluster_id} termination initiated in {region}",
            "response": terminate_response,
        }

    except Exception as e:
        return {"status": "failed", "error": str(e)}
