"""Register the Gold Delta tables in the Glue Data Catalog so Athena can query them.
Delta tables are registered as external tables with the symlink-manifest pattern *or* via a Glue crawler;
here we create a crawler with Delta targets (simplest, supports schema evolution).
    python -m aws.glue_catalog --role arn:aws:iam::<acct>:role/<GlueServiceRole>"""
import argparse

import boto3

from c360.config import settings as S

TABLES = ["customer_360", "abandoned_carts", "brand_sentiment", "cart_activity_windows"]


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--role", required=True)
    a = ap.parse_args()
    g = boto3.client("glue", region_name=S.aws_region)
    try:
        g.create_database(DatabaseInput={"Name": S.glue_db})
    except g.exceptions.AlreadyExistsException:
        pass
    targets = {"DeltaTargets": [{"DeltaTables": [f"s3://{S.s3_bucket}/gold/{t}/" for t in TABLES],
                                 "WriteManifest": False, "CreateNativeDeltaTable": True}]}
    try:
        g.create_crawler(Name="c360-gold", Role=a.role, DatabaseName=S.glue_db, Targets=targets,
                         SchemaChangePolicy={"UpdateBehavior": "UPDATE_IN_DATABASE", "DeleteBehavior": "LOG"})
    except g.exceptions.AlreadyExistsException:
        g.update_crawler(Name="c360-gold", Role=a.role, DatabaseName=S.glue_db, Targets=targets)
    g.start_crawler(Name="c360-gold")
    print("crawler started; tables will appear in database", S.glue_db)


if __name__ == "__main__":
    main()
