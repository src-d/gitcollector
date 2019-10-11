# gitcollector [![GitHub version](https://badge.fury.io/gh/src-d%2Fgitcollector.svg)](https://github.com/src-d/gitcollector/releases) [![Build Status](https://travis-ci.com/src-d/gitcollector.svg?branch=master)](https://travis-ci.com/src-d/gitcollector) [![codecov](https://codecov.io/gh/src-d/gitcollector/branch/master/graph/badge.svg)](https://codecov.io/gh/src-d/gitcollector) [![GoDoc](https://godoc.org/gopkg.in/src-d/gitcollector.v0?status.svg)](https://godoc.org/gopkg.in/src-d/gitcollector.v0) [![Go Report Card](https://goreportcard.com/badge/github.com/src-d/gitcollector)](https://goreportcard.com/report/github.com/src-d/gitcollector)

**gitcollector** collects and stores git repositories.

gitcollector is the source{d} tool to download and update git repositories at
large scale. To that end, it uses a custom repository storage
[file format](https://blog.sourced.tech/post/siva/) called [siva](https://github.com/src-d/go-siva) optimized for saving
storage space and keeping repositories up-to-date.

## Status

The project is in a preliminary stable stage and under active development.

## Storing repositories using rooted repositories

A rooted repository is a [bare Git repository](http://www.saintsjd.com/2011/01/what-is-a-bare-git-repository/) that stores all objects from all repositories that share a common history, that is, they have the same initial commit. It is stored using the [Siva](https://github.com/src-d/go-siva) file format.

![Root Repository explanatory diagram](https://user-images.githubusercontent.com/5582506/30617179-2aba194a-9d95-11e7-8fd5-0a87c2a595f9.png)

Rooted repositories have a few particularities that you should know to work with them effectively:

- They have no `HEAD` reference.
- All references are of the following form: `{REFERENCE_NAME}/{REMOTE_NAME}`. For example, the reference `refs/heads/master` of the remote `foo` would be `/refs/heads/master/foo`.
- Each remote represents a repository that shares the common history of the rooted repository. A remote can have multiple endpoints.
- A rooted repository is simply a repository with all the objects from all the repositories which share the same root commit.
- The root commit for a repository is obtained following the first parent of each commit from HEAD.

## Getting started

### Plain command

gitcollector entry point usage is done through the subcommand `download` (at this time is the only subcommand):

```txt
Usage:
  gitcollector [OPTIONS] download [download-OPTIONS]

Help Options:
  -h, --help                                     Show this help message

[download command options]
          --library=                             path where download to [$GITCOLLECTOR_LIBRARY]
          --bucket=                              library bucketization level (default: 2) [$GITCOLLECTOR_LIBRARY_BUCKET]
          --tmp=                                 directory to place generated temporal files (default: /tmp) [$GITCOLLECTOR_TMP]
          --workers=                             number of workers, default to GOMAXPROCS [$GITCOLLECTOR_WORKERS]
          --half-cpu                             set the number of workers to half of the set workers [$GITCOLLECTOR_HALF_CPU]
          --no-updates                           don't allow updates on already downloaded repositories [$GITCOLLECTOR_NO_UPDATES]
          --no-forks                             github forked repositories will not be downloaded [$GITCOLLECTOR_NO_FORKS]
          --orgs=                                list of github organization names separated by comma [$GITHUB_ORGANIZATIONS]
          --excluded-repos=                      list of repos to exclude separated by comma [$GITCOLLECTOR_EXCLUDED_REPOS]
          --token=                               github token [$GITHUB_TOKEN]
          --metrics-db=                          uri to a database where metrics will be sent [$GITCOLLECTOR_METRICS_DB_URI]
          --metrics-db-table=                    table name where the metrics will be added (default: gitcollector_metrics) [$GITCOLLECTOR_METRICS_DB_TABLE]
          --metrics-sync-timeout=                timeout in seconds to send metrics (default: 30) [$GITCOLLECTOR_METRICS_SYNC]

    Log Options:
          --log-level=[info|debug|warning|error] Logging level (default: info) [$LOG_LEVEL]
          --log-format=[text|json]               log format, defaults to text on a terminal and json otherwise [$LOG_FORMAT]
          --log-fields=                          default fields for the logger, specified in json [$LOG_FIELDS]
          --log-force-format                     ignore if it is running on a terminal or not [$LOG_FORCE_FORMAT]
```

Usage example, `--library` and `--orgs` are always required:

> gitcollector download --library=/path/to/repos/directoy --orgs=src-d

To collect repositories from several github organizations:

> gitcollector download --library=/path/to/repos/directoy --orgs=src-d,bblfsh

Note that all the download command options are also configurable with environment variables.

### Docker

gitcollector upload a new docker image to [docker hub](https://hub.docker.com/r/srcd/gitcollector/tags) on each new release. To use it:

``` sh
docker run --rm --name gitcollector_1 \
-e "GITHUB_ORGANIZATIONS=src-d,bblfsh" \
-e "GITHUB_TOKEN=foo" \
-v /path/to/repos/directory:/library \
srcd/gitcollector:latest
```

Note that you must mount a local directory into the specific container path shown in `-v /path/to/repos/directory:/library`. This directory is where the repositories will be downloaded into rooted repositories in siva files format.

## License

GPL v3.0, see [LICENSE](LICENSE)
