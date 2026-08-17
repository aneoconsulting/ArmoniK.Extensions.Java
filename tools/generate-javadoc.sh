#!/bin/sh

set -e

cd "$(dirname "$0")/.."

(cd armonik-client && ./mvnw --batch-mode javadoc:javadoc)
rm -rf .docs/_static/client
mkdir -p .docs/_static/client
cp -r armonik-client/target/reports/apidocs/. .docs/_static/client/

(cd worker && ./mvnw --batch-mode javadoc:aggregate)
rm -rf .docs/_static/worker
mkdir -p .docs/_static/worker
cp -r worker/target/reports/apidocs/. .docs/_static/worker/
