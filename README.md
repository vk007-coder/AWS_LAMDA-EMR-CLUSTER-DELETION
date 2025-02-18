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



