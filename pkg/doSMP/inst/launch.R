# This is intended to be called via system from the master process
# using the --args argument to the R command

args <- commandArgs(trailingOnly=TRUE)
if (length(args) != 3) {
  cat('usage: R -f launch.R --args qname rank verbose\n', file=stderr())
  quit('no')
}

# get the arguments
qname <- args[1]
rank <- as.integer(args[2])
verbose <- as.logical(args[3])

# set environment variables for the rank and verbosity
Sys.setenv(DOSMP_RANK=args[2])
Sys.setenv(DOSMP_VERBOSE=args[3])

# XXX control log directory via an option?
tmpdir <- if (verbose) getwd() else tempdir()
outfile <- tempfile(sprintf('SMP_%d_', rank), tmpdir=tmpdir)
out <- file(outfile, open='w')
on.exit(close(out))

# redirect foreach ads and user messages
sink(out)

# load doSMP, suppressing messages to avoid flooding the console
suppressMessages(library(doSMP))

if (verbose) {
  cat(sprintf('calling workerLoop: qname=%s rank=%d verbose=%s\n',
              qname, rank, verbose), file=out)
  flush(out)
}

# call the worker loop to execute tasks
doSMP:::workerLoop(qname, rank, verbose, out)

# finish up
cat(sprintf('worker %d exiting\n', rank), file=out)
flush(out)
quit('no')
