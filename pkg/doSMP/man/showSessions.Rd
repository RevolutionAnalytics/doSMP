\name{showSessions}
\alias{showSessions}
\title{showSessions}
\description{
The \code{showSessions} function is used to show all existing task queues.

}
\usage{
showSessions()
}
\value{
a character vector of task queue names, or \code{NULL}.
}
\details{
Usually, you know you need to call \code{showSessions} when you get a
warning message from \code{startWorkers} saying that there are other
worker groups.  This could be due to multiple concurrent doSMP sessions
(which could lead to lower performance), or it could be because
\code{stopWorkers} wasn't called at the end of a session. 

You can also call \code{showSessions} before calling \code{startWorkers} 
to see if there are any existing task queues. You can call \code{rmSessions}
with a call to \code{showSessions} as its only argument. This will remove
any existing sessions or, if there are no existing sessions, give a warning
that \code{rmSessions} did nothing.

}
\seealso{
  \code{\link{startWorkers}}, \code{\link{stopWorkers}}, \code{\link{rmSessions}}
}

\keyword{utilities}
