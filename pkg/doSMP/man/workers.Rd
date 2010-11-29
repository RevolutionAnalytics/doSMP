\name{workers}
\alias{startWorkers}
\alias{stopWorkers}
\title{Worker Management Functions}
\description{
The \code{startWorkers} function is used to start a specified number of
workers to execute tasks in parallel.  The \code{stopWorkers} function
is used to shutdown the workers in the specified worker group object.
}
\usage{
startWorkers(workerCount=getOption('cores'), FORCE=FALSE,
  tmpdir=tempdir(), verbose=FALSE)
stopWorkers(obj)
}
\arguments{
  \item{workerCount}{The number of worker processes to start on the local machine.
  The default is the value of \code{getOptions('cores')}.  If that isn't set,
  then three workers are started.}
  \item{FORCE}{A logical value indicating whether another worker group should be
  stopped in order to allow this group to be started.  This is useful if previous
  sessions were started, but not stopped properly.}
  \item{tmpdir}{The path of a directory in which to create temporary files.}
  \item{verbose}{A logical value indicating whether verbose messages should
  be displayed.}
  \item{obj}{The worker group object to stop.}
}
\details{
  The \code{startWorkers} function returns a "worker group" object, which
  represents the workers to evaluate tasks.  That object is passed to the
  \code{registerDoSMP} function for use by the \code{foreach} function.
  It is important to subsequently call the \code{stopWorkers} function,
  or the workers won't be stopped, but will keep running after your R session
  exits.  System IPC resources will also be retained.  This will cause
  subsequent calls to \code{startWorkers} to issue warning and error messages.
  IPC resources can be deleted using the \code{rmSessions} function.
}
\seealso{
  \code{\link{registerDoSMP}}, \code{\link{rmSessions}}
}
\examples{
# start two workers, compute some square roots, and shutdown
w <- startWorkers(2)
registerDoSMP(w)
foreach(i=1:3) \%dopar\% sqrt(i)
stopWorkers(w)
}
\keyword{utilities}
