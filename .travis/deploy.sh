#!/bin/bash

tag_version=$1

mkdir ~/.bintray/
FILE=$HOME/.bintray/.credentials
cat <<EOF >$FILE
realm = Bintray API Realm
host = api.bintray.com
user = $BINTRAY_USER
password = $BINTRAY_SNOWPLOW_MAVEN_API_KEY
EOF

cd $TRAVIS_BUILD_DIR
pwd

project_version=$(sbt version -Dsbt.log.noformat=true | perl -ne 'print $1 if /(\d+\.\d+[^\r\n]*)/')
sbt bintraySyncMavenCentral
