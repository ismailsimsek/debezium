#!/bin/bash

INSTALL_DIR="/Users/isimsek/Desktop"
VERSION="https://repo1.maven.org/maven2/io/debezium/debezium-server-dist/1.4.0.Beta1/debezium-server-dist-1.4.0.Beta1.tar.gz"
DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"

# Clean
rm -rf "${INSTALL_DIR:?}/debezium-server"
# download and extract
wget -c $VERSION -O - | tar -xz -C $INSTALL_DIR

cd "${DIR}" || exit
mvn clean install package -Passembly -DskipITs -DskipTests

cp -r "${DIR}/target"/debezium-server-batch-*-SNAPSHOT.jar "${INSTALL_DIR}/debezium-server/lib/"
cp -r "${DIR}/target"/debezium-server-batch-*-SNAPSHOT-runner.jar "${INSTALL_DIR}/debezium-server/lib/"
cp -r "${DIR}/target"/debezium-server-batch-*-SNAPSHOT-runner.jar "${INSTALL_DIR}/debezium-server/"
cp -r "${DIR}/target/lib"/* "${INSTALL_DIR}/debezium-server/lib/"

# @TODO deploy application.properties.example  conf