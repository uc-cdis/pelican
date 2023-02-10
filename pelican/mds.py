from datetime import datetime, timedelta
import requests


def metadata_submit_expiration(hostname, guid, access_token, record_expiration_days):
    url = f"{hostname}mds/metadata/{guid}"
    body = {"_expires_at": expires_at}
    print("-----------------------------------------------------")
    print(url)
    print(body)
    print("-----------------------------------------------------")

    expires_at = (datetime.now() + timedelta(days=record_expiration_days)).timestamp()
    r = requests.post(
        url,
        json=body,
        headers={"Authorization": f"bearer {access_token}"},
    )
    if r.status_code != 200:
        raise Exception(
            f"Submission to metadata-service failed with {r.status_code}:\n{r.text}"
        )
