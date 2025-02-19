# AWS Lambda EMR Cluster Terminator

## Overview

This AWS Lambda function dynamically identifies the region of an EMR cluster and terminates it. If the cluster is already terminated, it logs a message and exits gracefully.

## Features

- Detects the EMR cluster region dynamically.
- Checks if the cluster is already terminated before attempting to terminate it.
- Handles multiple AWS regions specified in environment variables.
- Logs relevant messages for tracking in AWS CloudWatch.

## Prerequisites

- **AWS Lambda** with Python runtime.
- **IAM Role** with permissions to describe and terminate EMR clusters.
- **Boto3 library** (included in AWS Lambda runtime by default).
- **CloudWatch Logs** for debugging and monitoring.

## Environment Variables

| Variable      | Description                                                                                 |
| ------------- | ------------------------------------------------------------------------------------------- |
| `EMR_REGIONS` | Comma-separated list of AWS regions to search for the EMR cluster. Defaults to `us-east-1`. |

## IAM Role Permissions

Create an IAM role with the following permissions to allow Lambda to describe and terminate EMR clusters:

```json
{
    "Version": "2012-10-17",
    "Statement": [
        {
            "Effect": "Allow",
            "Action": [
                "elasticmapreduce:DescribeCluster",
                "elasticmapreduce:TerminateJobFlows"
            ],
            "Resource": "*"
        }
    ]
}
```

## EMR Cluster Creation

### Creating an EMR Cluster via AWS Console:
1. Navigate to the **AWS EMR Console**.
2. Click **Create Cluster**.
3. Select **Go to advanced options**.
4. Choose applications like **Hadoop, Spark, Hive, etc.**
5. Configure the cluster with:
   - **Instance Type:** `m5.xlarge` (or as needed)
   - **Number of Instances:** `3` (or more based on workload)
6. Choose an appropriate **EC2 Key Pair** for SSH access.
7. Click **Create Cluster** and note down the **Cluster ID**.

### Creating an EMR Cluster via AWS CLI:
Use the following AWS CLI command to create an EMR cluster:

```sh
aws emr create-cluster \
    --name "MyEMRCluster" \
    --release-label emr-6.6.0 \
    --applications Name=Hadoop Name=Spark \
    --instance-groups InstanceGroupType=MASTER,InstanceCount=1,InstanceType=m5.xlarge \
                      InstanceGroupType=CORE,InstanceCount=2,InstanceType=m5.xlarge \
    --use-default-roles \
    --log-uri s3://my-emr-logs/
```

Replace `s3://my-emr-logs/` with your desired S3 bucket for logs.

## Deployment Steps

1. **Create the Lambda function:**
   - Use the AWS Lambda console or AWS CLI to create a new function.
   - Select Python as the runtime.
2. **Set up IAM Permissions:**
   - Attach a policy with `elasticmapreduce:DescribeCluster` and `elasticmapreduce:TerminateJobFlows` permissions.
3. **Upload the Lambda code:**
   - Copy and paste the Python script into the Lambda function.
4. **Configure Environment Variables:**
   - Set `EMR_REGIONS` (optional, defaults to `us-east-1`).
5. **Test the function:**
   - Use the following sample event:
   ```json
   {
     "ClusterId": "j-20GHZDIRHGQX8"
   }
   ```
   - Check CloudWatch Logs for execution results.

## Sample Response

### Case 1: Cluster Termination Initiated

```json
{
  "status": "success",
  "message": "EMR Cluster j-20GHZDIRHGQX8 termination initiated in us-east-1",
  "response": {...}
}
```

### Case 2: Cluster Already Terminated

```json
{
  "status": "info",
  "message": "EMR Cluster j-20GHZDIRHGQX8 is already terminated in us-east-1."
}
```

### Case 3: Cluster Not Found

```json
{
  "status": "failed",
  "message": "Cluster j-20GHZDIRHGQX8 not found in configured regions"
}
```

## Troubleshooting

- **Access Denied Errors:** Ensure your IAM role has the correct EMR permissions.
- **Invalid Region Error:** Check the `EMR_REGIONS` environment variable for whitespace or incorrect values.
- **Cluster Not Found:** Verify that the provided `ClusterId` is correct and the cluster exists.

.

