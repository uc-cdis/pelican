import requests

import boto3
from botocore.exceptions import ClientError
from botocore.config import Config


def s3upload_file(
    bucket, key, aws_access_key_id, aws_secret_access_key, filepath, expiration=3600
):
    """

    :param bucket: the destination bucket for the file upload
    :param aws_access_key_id:
    :param aws_secret_access_key:
    :param key: the name of the key in S3 for this upload
    :param filepath: local path to the file
    :param expiration:
    :return: presigned URL on success, False on failure
    """
    config = Config(signature_version="s3v4")
    client = boto3.client(
        "s3",
        aws_access_key_id=aws_access_key_id,
        aws_secret_access_key=aws_secret_access_key,
        config=config,
    )

    client.upload_file(filepath, bucket, key)
    try:
        response = client.generate_presigned_url(
            "get_object", Params={"Bucket": bucket, "Key": key}, ExpiresIn=expiration
        )
    except ClientError as _:
        return False

    return response


def s3download_file(
    bucket, key, aws_access_key_id, aws_secret_access_key, fileobj=None, filepath=None
):
    """

    :param bucket: the source bucket for the file download
    :param aws_access_key_id:
    :param aws_secret_access_key:
    :param key: the name of the key in S3 for this download
    :param fileobj: file-like object to download into
    :param filepath: local path to the file
    :return:
    """
    client = boto3.client(
        "s3",
        aws_access_key_id=aws_access_key_id,
        aws_secret_access_key=aws_secret_access_key,
    )

    assert (fileobj is None and filepath is not None) or (
        fileobj is not None and filepath is None
    ), "both arguments can't be specified"

    if fileobj:
        client.download_fileobj(bucket, key, fileobj)
    if filepath:
        client.download_file(bucket, key, filepath)
    return


def download_file(url, fileobj=None, filepath=None):
    r = requests.get(url)

    assert (fileobj is None and filepath is not None) or (
        fileobj is not None and filepath is None
    ), "both arguments can't be specified"

    if fileobj:
        fileobj.write(r.content)
    if filepath:
        with open(filepath, "wb") as f:
            f.write(r.content)

    return
