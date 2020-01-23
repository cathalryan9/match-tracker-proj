#!/bin/sh

DIR=""../../../media/sf_eclipse-workspace/match-tracker-proj/dashboard-client"
cp $DIR/src $DIR/.gitignore $DIR/package.json $DIR/README.md $DIR/copy.sh . -r
dos2unix copy.sh
