library(doSMP)

# start workers in verbose mode to see the worker output
# in the worker log files
verbose <- as.logical(Sys.getenv('VERBOSE', 'FALSE'))
w <- startWorkers(verbose=verbose)

# register doSMP to be used with %dopar%
registerDoSMP(w)

# define the function to initialize each worker's environment
# before executing the tasks for a foreach
initEnvir <- function(e, tempname) {
  fname <- sprintf(tempname, Sys.getenv('DOSMP_RANK'))
  cat(sprintf('initEnvir creating file: %s\n', fname))
  e$outfile <- file(fname, 'w')
}

# define the function to finalize each worker's environment
# after executing the tasks for a foreach
finalEnvir <- function(e) {
  cat('finalEnvir called\n')
  close(e$outfile)
}

# initialize the doSMP-specific options
randint <- as.integer(runif(1, max=1000000))
tempname <- sprintf('test_%%s_%d%d.out', Sys.getpid(), randint)
opts <- list(initEnvir=initEnvir, initArgs=list(tempname), finalEnvir=finalEnvir)

# all the workers do is to write to their own output file
ntasks <- 100
foreach(i=icount(ntasks), .options.smp=opts) %dopar% {
  cat(sprintf('worker %s got task %d\n', Sys.getenv('DOSMP_RANK'), i),
      file=outfile)
}

# get all possible output files
outfiles <- sprintf(tempname, seq(length=getDoParWorkers()))

# filter out files if worker didn't get any tasks
outfiles <- outfiles[file.exists(outfiles)]

# at least one worker must have executed tasks
stopifnot(length(outfiles) > 0)

# print list of output files
print(outfiles)

# get total number of lines in all of the output files
n <- foreach(f=outfiles, .combine='+') %do% length(readLines(f))
cat(sprintf('total lines in output files: %d; number of tasks: %d\n',
            n, ntasks))
if (n != ntasks)
  warning('output files have incorrect number of lines')

# delete output files
if (! verbose)
  file.remove(outfiles)

# shutdown the workers
stopWorkers(w)
