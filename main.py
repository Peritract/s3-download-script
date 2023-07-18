"""Utility script for downloading files from an S3 bucket."""

import os

from boto3 import client
from botocore.client import BaseClient
from dotenv import dotenv_values


def get_bucket_names(s3_client: BaseClient) -> list[str]:
    """Returns the list of bucket names available."""

    if not isinstance(s3_client, BaseClient):
        raise TypeError("Invalid argument: s3_client must be a boto3 client")

    return [b["Name"] for b in s3_client.list_buckets()["Buckets"]]


def get_all_items_in_bucket(s3_client: BaseClient, bucket_name: str) -> list[str]:
    """Returns a list of object keys from a bucket."""

    return [o["Key"] for o in s3_client.list_objects(Bucket=bucket_name)["Contents"]]


def download_file_from_bucket(s3_client: BaseClient, bucket_name: str,
                              object_key: str, folder: str=".") -> None:
    """Downloads a given file from a given bucket"""

    s3_client.download_file(bucket_name, object_key, f"{folder}/{object_key}")


def download_all_files_from_bucket(s3_client: BaseClient, bucket_name: str,
                                   folder: str=".") -> None:
    """Downloads all files from a given bucket to the specified folder."""

    files = get_all_items_in_bucket(s3_client, bucket_name)

    if folder != "." and not os.path.exists(folder):
        os.mkdir(folder)

    for file in files:
        download_file_from_bucket(s3_client, bucket_name, file, folder)


if __name__ == "__main__": # All actual actions happen here

    config = dotenv_values()

    s3 = client("s3", aws_access_key_id=config["ACCESS_KEY_ID"],
                aws_secret_access_key=config["SECRET_ACCESS_KEY"])

    download_all_files_from_bucket(s3, config["BUCKET_NAME"], config["DST_FOLDER"])
