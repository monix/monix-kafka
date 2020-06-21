#!/usr/bin/env bash

set -e

echo "GIT_DEPLOY_KEY: $GIT_DEPLOY_KEY"

sbt docs/docusaurusPublishGhpages