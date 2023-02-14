# PFB Export

The `pelican-export` job takes a Guppy filter and a user's access token as input. It queries Guppy for the list of cases for that filter. The output can be a URL or the GUID for an [Indexd](https://github.com/uc-cdis/indexd) record.

## Launching the job

In this section, we assume the job is already configured (if not, see [Setup](#setup) section).

Start the Sower job by hitting the `POST <base URL>/job/dispatch` endpoint with the body below.

```
{
  "action": "export",
  "access_format": "str",
  "input": {
    "filter": { "=": { "project_id": "my_program-my_project" } },
    "root_node": "my_node"
  }
}
```

- `action`: `"export"`
- `input`: `{"filter": { <Guppy filter> }, "root_node": "str" }`.
  - `filter`: optional; defaults to `{}`. See Guppy documentation about filters [here](https://github.com/uc-cdis/guppy/blob/master/doc/queries.md#filter).
  - `root_node`: optional; name of the root node in the graph data model. If omitted, Pelican assumes it is the same as the name of the index configured as `ROOT_NODE` in the Sower configuration (see [Setup](#setup) section).
- `access_format`: optional. Set it to `"guid"` to index exported PFBs in [Indexd](https://github.com/uc-cdis/indexd). By default, the job returns a URL to the exported PFB.

## Setup

1. This job should be deployed alongside the `metadata-delete-expired-objects` cronjob:
    - Add `"metadata-delete-expired-objects": "quay.io/cdis/metadata-delete-expired-objects:<version>"` to the `versions` block of the manifest.
    - Run `gen3 kube-setup-metadata-delete-expired-objects-cronjob`.
    - Grant the `metadata-delete-expired-objects-job` client access to `(resource=/mds_gateway, method=access, service=mds_gateway)` and `(resource=/programs, method=delete, service=fence)` in the `user.yaml`.
2. Run `gen3 kube-setup-pelicanjob`. This will create the S3 bucket for exported PFB files and the Fence client for submitting to the metadata service.
3. Grant the `pelican-export-job` client access to `(resource=/mds_gateway, method=access, service=mds_gateway)` in the `user.yaml`.
4. Set up and deploy [Sower](https://github.com/uc-cdis/sower).
5. Update the manifest to include the configuration for the `pelican-export` job:

##### Manifest configuration

The `pelican-export` job should have the following environment variables and mounts set:

* Environment variable:
    * `DICTIONARY_URL`
    * `GEN3_HOSTNAME`
    * `ROOT_NODE`
    * `EXTRA_NODES` an optional comma-delimited list of nodes to additionally include in the PFB.
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
      },
      {
        "name": "EXTRA_NODES",
        "value": "reference_file,reference_file_index"
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

## Architecture

<a href="https://www.lucidchart.com/publicSegments/view/78ed9fc6-6ab4-4035-8bda-9bd4269cce05/image.png"><img src="https://www.lucidchart.com/publicSegments/view/78ed9fc6-6ab4-4035-8bda-9bd4269cce05/image.png" width="600" /></a>
