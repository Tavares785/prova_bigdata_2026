"""Operações S3 via AWS CLI v2, sem criar roles nem usar aws_s3_bucket."""
import json
import os
import subprocess
import sys


def aws(*args, allow_error=False):
    result = subprocess.run(["aws", *args, "--region", "us-east-1", "--no-cli-pager"],
                            text=True, capture_output=True)
    if result.returncode and not allow_error:
        raise RuntimeError(f"AWS CLI falhou ({args[0]} {args[1]}): {result.stderr.strip()}")
    return result


def main():
    action = sys.argv[1]
    bucket = os.environ["BUCKET_NAME"]
    owner = os.environ["ACCOUNT_ID"]
    if action == "create":
        result = aws("s3api", "create-bucket", "--bucket", bucket, allow_error=True)
        if result.returncode and "BucketAlreadyOwnedByYou" not in result.stderr:
            raise RuntimeError(result.stderr.strip())
        aws("s3api", "head-bucket", "--bucket", bucket, "--expected-bucket-owner", owner)
        aws("s3api", "put-public-access-block", "--bucket", bucket,
            "--expected-bucket-owner", owner, "--public-access-block-configuration",
            "BlockPublicAcls=true,IgnorePublicAcls=true,BlockPublicPolicy=true,RestrictPublicBuckets=true")
        aws("s3api", "put-bucket-encryption", "--bucket", bucket,
            "--expected-bucket-owner", owner, "--server-side-encryption-configuration",
            json.dumps({"Rules": [{"ApplyServerSideEncryptionByDefault": {"SSEAlgorithm": "AES256"}}]}))
    if action in ("create", "tag"):
        tags = json.loads(os.environ["BUCKET_TAGS"])
        aws("s3api", "put-bucket-tagging", "--bucket", bucket,
            "--expected-bucket-owner", owner,
            "--tagging", json.dumps({"TagSet": [{"Key": k, "Value": v} for k, v in tags.items()]}))
    elif action == "delete":
        check = aws("s3api", "head-bucket", "--bucket", bucket,
                    "--expected-bucket-owner", owner, allow_error=True)
        if check.returncode and ("(404)" in check.stderr or "NoSuchBucket" in check.stderr):
            return
        if check.returncode:
            raise RuntimeError(check.stderr.strip())
        aws("s3", "rb", f"s3://{bucket}", "--force")
    else:
        raise ValueError(f"Ação desconhecida: {action}")


if __name__ == "__main__":
    main()
