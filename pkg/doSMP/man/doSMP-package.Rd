\name{doSMP-package}
\alias{doSMP-package}
\alias{doSMP}
\docType{package}
\title{
The doSMP Package
}
\description{
The doSMP package provides a parallel backend for the foreach/\%dopar\%
function.  It executes tasks on a single, multiprocess/multicore machine.
}
\details{
The following options are supported by the doSMP backend.  They are specified
with a list via the foreach \code{.options.smp} argument:

\code{chunkSize} specifies the number of tasks to be sent to the workers at a time.

\code{info} is a logical value indicating that extra information should be
displayed.

\code{initEnvir} is a function to be executed by each worker before any tasks
are executed associated with a foreach.  Its purpose is to initialize the
execution environment, or the worker in general.  It is only executed by a
worker if that worker executes at least one task associated with the foreach.
The first argument passed to the function is the execution environment.  The
remaining arguments are specified via the initArgs argument.

\code{initArgs} specifies a list of arguments to be passed to the initEnvir
function after the environment argument.

\code{finalEnvir} is a function to be executed by each worker after all tasks
are executed associated with a foreach.  Its purpose is to clean up the
execution environment environment, or the worker in general.  It is only
executed by a worker if that worker executed at least one task associated with
the foreach.  The first argument is the execution environment.  The remaining
arguments are specified via the finalArgs argument.

\code{finalArgs} specifies a list of arguments to be passed to the finalEnvir
function after the environment argument.

Further information is available in the following help topics:
\tabular{ll}{
\code{startWorkers} \tab start worker processes\cr
\code{stopWorkers} \tab shutdown worker processes\cr
\code{registerDoSMP} \tab register doSMP to be used by foreach/\%dopar\%\cr
\code{rmSessions} \tab cleanup orphaned doSMP sessions\cr
}

To see a tutorial introduction to the doSMP package,
use \code{vignette("gettingstartedSMP")}.  To see a tutorial
introduction to the foreach package, use \code{vignette("foreach")}.

To see a demo of doSMP computing the sinc function,
use \code{demo(sincSMP)}.

Some examples (in addition to those in the help pages) are included in
the ``examples'' directory of the doSMP package.  To list the files in
the examples directory,
use \code{list.files(system.file("examples", package="doSMP"))}.
To run the bootstrap example, use
\code{source(system.file("examples", "bootSMP.R", package="doSMP"))}.
This is a simple benchmark, executing both sequentally and in parallel.
There are many more examples that come with the foreach package,
which will work with the doSMP package if it is registered as the
parallel backend.

For a complete list of functions with individual help pages,
use \code{library(help="doSMP")}.
}
\keyword{package}
