#!/bin/sh

LOGFILE=${FOREACH_LOGFILE:-'test.log'}

R --vanilla --slave > ${LOGFILE} 2>&1 <<'EOF'
library(doSMP)
library(RUnit)

options(warn=1)
options(showWarnCalls=TRUE)

cat('Starting test at', date(), '\n')
cat(sprintf('doSMP version: %s\n', getDoParVersion()))
cat(sprintf('Running with %d worker(s)\n', getDoParWorkers()))

tests <- c('options.R', 'stressTest.R')

errcase <- list()
for (f in tests) {
  cat('\nRunning test file:', f, '\n')
  t <- runTestFile(f)
  e <- getErrors(t)
  if (e$nErr + e$nFail > 0) {
    errcase <- c(errcase, t)
    print(t)
  }
}

if (length(errcase) == 0) {
  cat('*** Ran all tests successfully ***\n')
} else {
  cat('!!! Encountered', length(errcase), 'problems !!!\n')
  for (t in errcase) {
    print(t)
  }
}

cat('Finished test at', date(), '\n')
EOF
