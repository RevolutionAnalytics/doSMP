\name{registerDoSMP}
\alias{registerDoSMP}
\title{registerDoSMP}
\description{
The \code{registerDoSMP} function is used to register the SMP parallel backend
with the foreach package.  This backend runs only on the local machine, making
use of multiple processors/cores to run in parallel.
}
\usage{
registerDoSMP(w)
}
\arguments{
  \item{w}{A worker group object returned by the \code{startWorkers} function.}
}
\seealso{
  \code{\link{startWorkers}}
}

\keyword{utilities}
