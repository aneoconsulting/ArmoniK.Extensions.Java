#!/bin/sh

set -e

cd "$(dirname "$0")/.."

# On Read the Docs, `java` on PATH is not switched by installing the JDK
# apt package (only javac/javadoc/etc. are), so Maven ends up running under
# an older JVM than the one it compiles with. Derive JAVA_HOME from javac,
# which update-alternatives does point at the installed JDK, so mvnw runs
# under the same JDK it forks javac from.
if [ -z "$JAVA_HOME" ]; then
  JAVA_HOME="$(dirname "$(dirname "$(readlink -f "$(command -v javac)")")")"
  export JAVA_HOME
fi

(cd armonik-client && ./mvnw --batch-mode javadoc:javadoc)
rm -rf .docs/_static/client
mkdir -p .docs/_static/client
cp -r armonik-client/target/reports/apidocs/. .docs/_static/client/

(cd worker && ./mvnw --batch-mode javadoc:aggregate)
rm -rf .docs/_static/worker
mkdir -p .docs/_static/worker
cp -r worker/target/reports/apidocs/. .docs/_static/worker/
