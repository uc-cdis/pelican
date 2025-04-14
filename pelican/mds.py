from datetime import datetime, timedelta
import requests
from pelican.config import logger


def metadata_submit_expiration(hostname, guid, access_token, record_expiration_days):
    expires_at = (datetime.now() + timedelta(days=record_expiration_days)).timestamp()
    url = f"{hostname}mds/metadata/{guid}"
    body = {"_expires_at": expires_at}
    logger.info("-----------------------------------------------------")
    logger.info(url)
    logger.info(body)
    logger.info("-----------------------------------------------------")
    r = requests.post(
        url,
        json=body,
        headers={"Authorization": f"bearer {access_token}"},
    )
    if r.status_code != 201:
        raise Exception(
            f"Submission to metadata-service failed with {r.status_code}:\n{r.text}"
        )
