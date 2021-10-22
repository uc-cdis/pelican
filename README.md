# Pelican

Provides Docker images with [Sower](https://github.com/uc-cdis/sower) jobs to export and import PFB in Gen3.

## PFB Export [![Docker Repository on Quay](https://quay.io/repository/cdis/pelican-export/status "Docker Repository on Quay")](https://quay.io/repository/cdis/pelican-export)

The `pelican-export` job takes a Guppy filter and a user's access token as input. It queries Guppy for the list of cases for that filter. The output can be a URL or the GUID for an [Indexd](https://github.com/uc-cdis/indexd) record.

[See configuration instructions](docs/pelican-export.md)

## PFB Import [![Docker Repository on Quay](https://quay.io/repository/cdis/pelican-import/status "Docker Repository on Quay")](https://quay.io/repository/cdis/pelican-import)

The `pelican-import` job is only used to restore a PFB into a clean database.

[See configuration instructions](docs/pelican-import.md)

## Release

To create a new release, just add a `release` label to your PR and a GitHub Action will be triggered.
