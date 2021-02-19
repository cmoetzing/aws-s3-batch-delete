# aws-s3-batch-delete
Deleting objects from S3 buckets can be painful. Using the `--recursive` flag of `aws s3 rm` fires one request per entry.
This tool uses the `DeleteObjectsRequest` request to delete 1000 objects at a time with several threads running simultaneously.

## Running
```bash
usage: aws-s3-batch-delete.jar --bucket <bucket> [--prefix <prefix>]
       --profile <profile> --region <region> [--serviceEndpoint
       <serviceEndpoint>] [--threads <threads>]
    --bucket <bucket>                     the AWS bucket
    --prefix <prefix>                     the AWS prefix (or subfolder),
                                          if not provided the whole
                                          content of the bucket will be
                                          deleted
    --profile <profile>                   the AWS credentials profile
    --region <region>                     the AWS region
    --serviceEndpoint <serviceEndpoint>   the AWS service endpoint
    --threads <threads>                   the number of parallel requests,
                                          default is 4
```

## Parallelism
More than 4 threads seem to exceed the AWS API request limit.