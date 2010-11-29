#!/bin/bash

if [ x"$REXECUTABLE" == x ]; then
    REXECUTABLE=R
fi

if [ x"$RARGUMENTS" == x ]; then
    RARGUMENTS="--vanilla --slave"
fi

if [ x"$RTESTLOG" == x ]; then
    RTESTLOG="ripctest.log"
fi

exec 1>"$RTESTLOG"

if "${REXECUTABLE}" "${RARGUMENTS}" < testrunner.R; then
    if [ -t 2 ]; then
        echo -e '\x1b[1;32mSuccess\x1b[0m' >&2
    else
        echo -e 'Success' >&2
    fi
    exit 0
else
    if [ -t 1 ]; then
        echo -e '\x1b[1;31mFailure\x1b[0m' >&2
    else
        echo -e 'Failure' >&2
    fi
    echo "See ${RTESTLOG} for details." >&2
    exit 1
fi
