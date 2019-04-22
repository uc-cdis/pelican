import boto3
from botocore.exceptions import ClientError


def s3upload_file(
    bucket, key, aws_access_key_id, aws_secret_access_key, filepath, expiration=3600
):
    """

    :param bucket: bucket to upload into
    :param aws_access_key_id:
    :param aws_secret_access_key:
    :param key: the name of the key in S3 for this upload
    :param filepath: local path to file
    :param expiration:
    :return: presigned URL on success, False on failure
    """
    client = boto3.client(
        "s3",
        aws_access_key_id=aws_access_key_id,
        aws_secret_access_key=aws_secret_access_key,
    )

    client.upload_file(filepath, bucket, key)
    try:
        response = client.generate_presigned_url(
            "get_object", Params={"Bucket": bucket, "Key": key}, ExpiresIn=expiration
        )
    except ClientError as _:
        return False

    # the response contains the presigned URL
    return response
