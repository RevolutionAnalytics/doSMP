\name{rmSessions}
\alias{rmSessions}
\title{rmSessions}
\description{
The \code{rmSessions} function is used to delete task queues that
weren't properly destroyed.  A task queue is created by the
\code{startWorkers} function, and is deleted by the \code{stopWorkers}
function.  If \code{stopWorkers} isn't called, then the interprocess
communication (IPC) resources used to implement the task queue will
still exist after the R session is finished.  This is known as a
resource leak, and can become a problem.  The leaked resources should be
cleaned up after the machine is rebooted, but this function allows the
resources from a previous R session to be deleted without needing to
reboot the machine.

Usually, you know you need to call \code{rmSessions} when you get a
warning message from \code{startWorkers} saying that there are other
worker groups.  This could be due to multiple concurrent doSMP sessions
(which could lead to lower performance), or it could be because
\code{stopWorkers} wasn't called at the end of a session.
}
\usage{
rmSessions(qnames=NULL, all.names=FALSE)
}
\arguments{
  \item{qnames}{vector of queue names to delete.  Legal values are:
  doSMP1, doSMP2, doSMP3, doSMP4, doSMP5, doSMP6, doSMP7, doSMP8.}
  \item{all.names}{a logical value.  If \code{TRUE}, all existing
  queues will be deleted.  This is a short cut, and should not be set
  to \code{TRUE} if \code{qnames} is specified.}
}
\seealso{
  \code{\link{startWorkers}}, \code{\link{stopWorkers}}
}

\keyword{utilities}
