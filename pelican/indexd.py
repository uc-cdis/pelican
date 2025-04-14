import requests
import json
from pelican.config import logger


def indexd_submit(
    hostname, access_token, file_name, size, urls, hashes, authz=None, did=None
):
    """
    :params hostname: url of the commons
    "params access_token: token for submitting files to indexd"
    :params file_name: path to file
    :params size: size of the file
    :parmas urls: array of urls of the file (S3 urls)
    :params hashes: dictionary of file hashes {s3: 1234}
    :params did: UUID of the file, Optional as indexd will create a did if not specified
    """

    body = {}
    body["file_name"] = file_name
    body["size"] = size
    body["urls"] = urls
    body["hashes"] = hashes
    body["form"] = "object"

    if authz:
        body["authz"] = authz
    if did:
        body["did"] = did

    indexd_hostname = hostname + "index/index"

    logger.info("-----------------------------------------------------")
    logger.info(indexd_hostname)
    logger.info(json.dumps(body))
    logger.info("-----------------------------------------------------")

    r = requests.post(
        indexd_hostname,
        data=json.dumps(body),
        headers={"content-type": "application/json"},
        auth=("gdcapi", str(access_token)),
    )

    if r.status_code == 200:
        return r.json()
    else:
        raise Exception(
            f"Submission to indexd failed with {r.status_code} ------- {r.text}"
        )
