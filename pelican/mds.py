from datetime import datetime, timedelta
import requests


def metadata_submit(hostname, guid, access_token, record_expiration_days):
    expires_at = (datetime.now() + timedelta(days=record_expiration_days)).timestamp()
    r = requests.post(
        f"{hostname}mds/metadata/{guid}",
        json={"_expires_at": expires_at},
        # headers={"content-type": "application/json"},
        # auth=("gdcapi", str(access_token)),
        headers={"Authorization": f"bearer {access_token}"},
    )
    if r.status_code != 200:
        raise Exception(
            f"Submission to metadata-service failed with {r.status_code}:\n{r.text}"
        )
