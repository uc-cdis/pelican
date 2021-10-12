# PFB Import

The `pelican-import` job is only used to restore a PFB into a clean database.

## Launching the job

In this section, we assume the job is already configured (if not, see [Setup](#setup) section).

Start the Sower job by hitting the `POST <base URL>/job/dispatch` endpoint with the body below.

```
{
  "action": "import",
  <TODO>
}
```

## Setup

_Note_: There are no prerequisites for the `pelican-import` job.

1. Update the manifest to include the configuration for the `pelican-import` job:

##### Manifest configuration

* Environment variables:
    * `DICTIONARY_URL`
    * `GEN3_HOSTNAME`
* Mounts:
    * `sheepdog-creds-volume` - the secret with write access to the sheepdog database.

```
{
  "name": "pelican-import",
  "action": "import",
  "container": {
    "name": "job-task",
    "image": "quay.io/cdis/pelican-import:master",
    "pull_policy": "Always",
    "env": [
      {
        "name": "DICTIONARY_URL",
        "valueFrom": {
          "configMapKeyRef": {
            "name": "manifest-global",
            "key": "dictionary_url"
          }
        }
      },
      {
        "name": "GEN3_HOSTNAME",
        "valueFrom": {
          "configMapKeyRef": {
            "name": "manifest-global",
            "key": "hostname"
          }
        }
      }
    ],
    "volumeMounts": [
      {
        "name": "sheepdog-creds-volume",
        "readOnly": true,
        "mountPath": "/sheepdog-creds.json",
        "subPath": "creds.json"
      }
    ],
    "cpu-limit": "1",
    "memory-limit": "4Gi"
  },
  "volumes": [
    {
      "name": "sheepdog-creds-volume",
      "secret": {
        "secretName": "sheepdog-creds"
      }
    }
  ],
  "restart_policy": "Never"
}
```

## Architecture

<a href="https://www.lucidchart.com/publicSegments/view/8d612284-709e-4789-b9ae-0a23351b82a7/image.png"><img src="https://www.lucidchart.com/publicSegments/view/8d612284-709e-4789-b9ae-0a23351b82a7/image.png" width="600" /></a>
