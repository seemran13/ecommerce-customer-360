"""Upload raw landing + Delta/Parquet lake folders to S3 (cloud raw zone).  Uses boto3 default credentials.
    python -m aws.s3_sync --src data/lake/raw --prefix raw"""
import argparse
import os

import boto3

from c360.config import settings as S


def sync(src, prefix, bucket=S.s3_bucket):
    s3 = boto3.client("s3", region_name=S.aws_region)
    n = 0
    for root, _, files in os.walk(src):
        for f in files:
            full = os.path.join(root, f)
            key = f"{prefix}/{os.path.relpath(full, src)}".replace(os.sep, "/")
            s3.upload_file(full, bucket, key)
            n += 1
    print(f"uploaded {n} files to s3://{bucket}/{prefix}/")


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--src", required=True)
    ap.add_argument("--prefix", required=True)
    a = ap.parse_args()
    sync(a.src, a.prefix)
