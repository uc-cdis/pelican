# Pelican

Provides Docker images with Sower jobs to export and import PFB in Gen3.

## PFB Export [![Docker Repository on Quay](https://quay.io/repository/cdis/pelican-export/status "Docker Repository on Quay")](https://quay.io/repository/cdis/pelican-export)

`pelican-export` job takes Guppy query and users access token as an input. Then it queries Guppy for the list of cases for that query. Can output as a url or an IndexD record.

### Setup

1. Run `gen3 kube-setup-pelicanjob`. This will create the S3 bucket for exported PFB.
2. Setup and install `sower`.
3. Update manifest to include configuration for `pelican-export` job: 
<details>
  <summary>Manifest configuration</summary>
The `pelican-export` job should have the following environment variables and mounts set:

* Environment variable:
    * `DICTIONARY_URL`
    * `GEN3_HOSTNAME`
    * `ROOT_NODE`
* Mounts:
    * `pelican-creds-volume` - the secret from `kube-setup-pelicanjob`
    * `peregrine-creds-volume` - the secret to access sheepdog database.
    * `indexd-creds-volume` - the secret to access indexd submissions

```
{
  "name": "pelican-export",
  "action": "export",
  "container": {
    "name": "job-task",
    "image": "quay.io/cdis/pelican-export:master",
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
      },
      {
        "name": "ROOT_NODE",
        "value": "subject"
      }
    ],
    "volumeMounts": [
      {
        "name": "pelican-creds-volume",
        "readOnly": true,
        "mountPath": "/pelican-creds.json",
        "subPath": "config.json"
      },
      {
        "name": "peregrine-creds-volume",
        "readOnly": true,
        "mountPath": "/peregrine-creds.json",
        "subPath": "creds.json"
      },
      {
        "name": "indexd-creds-volume",
        "readOnly": true,
        "mountPath": "/indexd-creds.json",
        "subPath": "creds.json"
      }
    ],
    "cpu-limit": "1",
    "memory-limit": "12Gi"
  },
  "volumes": [
    {
      "name": "pelican-creds-volume",
      "secret": {
        "secretName": "pelicanservice-g3auto"
      }
    },
    {
      "name": "peregrine-creds-volume",
      "secret": {
        "secretName": "peregrine-creds"
      }
    },
    {
      "name": "indexd-creds-volume",
      "secret": {
        "secretName": "indexd-creds"
      }
    }
  ],
  "restart_policy": "Never"
}
```
</details>

### Architecture

<a href="https://www.lucidchart.com/publicSegments/view/78ed9fc6-6ab4-4035-8bda-9bd4269cce05/image.png"><img src="https://www.lucidchart.com/publicSegments/view/78ed9fc6-6ab4-4035-8bda-9bd4269cce05/image.png" width="600" /></a>

## PFB Import [![Docker Repository on Quay](https://quay.io/repository/cdis/pelican-import/status "Docker Repository on Quay")](https://quay.io/repository/cdis/pelican-import)

`pelican-import` job now used only for restoring PFB into the clean database.

### Setup

_Note_: There is no prerequisities for `pelican-import` job.

1. Setup manifest configuration:
<details>
The manifest configuration should include:

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
</details>

### Architecture

<a href="https://www.lucidchart.com/publicSegments/view/8d612284-709e-4789-b9ae-0a23351b82a7/image.png"><img src="https://www.lucidchart.com/publicSegments/view/8d612284-709e-4789-b9ae-0a23351b82a7/image.png" width="600" /></a>

### Release

To produce a new release just add a `release` label to your PR and a Github Action will be triggered.
