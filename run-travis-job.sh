#!/bin/sh

set -xe

make $COMMAND

[ "$COV" = "yes" ] && \
    cargo install cargo-tarpaulin && \
    cargo tarpaulin -v --ignore-tests \
        --ciserver travis-ci --coveralls "$TRAVIS_JOB_ID"
