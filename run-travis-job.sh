#!/bin/sh

set -xe

make $COMMAND

if [ "$COV" = "yes" ]
then
    cargo install cargo-tarpaulin || travis_terminate 0
    cargo tarpaulin -v --ignore-tests \
        --ciserver travis-ci --coveralls "$TRAVIS_JOB_ID"
fi
