QNAMES <- sprintf('doSMP%d', 1:8)

startWorkers <- function(workerCount=getOption('cores'), FORCE=FALSE,
                         tmpdir=tempdir(), verbose=FALSE) {
  # get number of workers to start
  if (is.null(workerCount))
    workerCount <- 3
  workerCount <- as.integer(workerCount)

  # get path to R and the worker launch script
  rexe <- if (.Platform$OS.type == 'windows') 'Rterm.exe' else 'R'
  rpath <- file.path(R.home('bin'), rexe)
  script <- system.file('launch.R', package='doSMP')

  # create a new directory in the specified temp directory
  # since the "task queue" wants complete control over it
  tmpdir <- tempfile(pattern="doSMP", tmpdir=tmpdir)
  dir.create(tmpdir)

  # try to create a task queue
  for (qname in QNAMES) {
    taskq <- tryCatch(ipcTaskQueueCreate(qname, tmpdir),
                      error=function(e) NULL)
    if (!is.null(taskq))
      break

    # warn the user about the other session
    warning(sprintf('there is an existing doSMP session using %s', qname))
  }

  # check if we successfully created a task queue
  if (is.null(taskq)) {
    # if FORCE is false, throw an error
    if (!FORCE) {
      warning('possible leak of worker sessions: consider using FORCE=TRUE')
      stop('unable to create a task queue: limit exceeded')
    }

    # try to destroy and recreate one of the existing task queues
    for (qname in QNAMES) {
      if (rmSession(qname)) {
        taskq <- tryCatch(ipcTaskQueueCreate(qname, tmpdir),
                          error=function(e) NULL)
        if (!is.null(taskq))
          break

        warning(sprintf('unable to recreate task queue %s', qname))
      } else {
        warning(sprintf('unable to destroy task queue %s', qname))
      }
    }
  }

  # if we still didn't succeed, give up
  if (is.null(taskq))
    stop('unable to destroy and recreate a task queue')

  # start the workers
  for (rank in seq(length=workerCount)) {
    argv <- c(rpath, '--vanilla', '--slave', '-f', script,
              '--args', qname, as.character(rank), as.character(verbose))
    cmd <- argv2str(argv, NULL)  # default quoting works for R

    if (verbose) {
      cat(sprintf('executing command to start worker %d:\n', rank))
      cat(sprintf('  %s\n', cmd))
    }

    # I'm using the input argument only on Windows because
    # I believe this is the better tested method
    if (.Platform$OS.type == 'windows')
      system(cmd, wait=FALSE, input='')
    else
      system(cmd, wait=FALSE)
  }

  # construct and return the "workergroup" object
  obj <- list(taskq=taskq, qname=qname, workerCount=workerCount,
              tmpdir=tmpdir, verbose=verbose)
  class(obj) <- c('nofork', 'workergroup')
  obj
}

stopWorkers <- function(obj) {
  # send a poison pill to each of the workers
  if (obj$verbose) {
    cat('sending shutdown request to the workers\n')
    flush(stdout())
  }
  w <- seq(length=obj$workerCount)
  for (i in w)
    ipcTaskStore(obj$taskq, NULL)

  # wait for them to die
  if (obj$verbose) {
    cat('waiting for the workers to reply to shutdown request\n')
    flush(stdout())
  }
  FUN <- function(i) identical(ipcTaskWaitResult(obj$taskq)$data, NULL)
  status <- sapply(w, FUN)

  # only destroy the task queue if everything looks sane
  s <- if (all(status)) {
    if (obj$verbose) {
      cat('destroying the task queue\n')
      flush(stdout())
    }

    # only a short delay since the workers have already replied to
    # the poison pill task
    destroyTaskQueue(obj$taskq, 1)
  } else {
    if (obj$verbose) {
      cat('got bad shutdown replies from the task queue\n')
      flush(stdout())
    }
    warning('task queue appears corrupt: not destroying')
    FALSE
  }
  invisible(s)
}

showSessions <- function()
{
  val <- NULL
  for (qname in QNAMES) {
    tryCatch({
      taskq <- ipcTaskQueueOpen(qname)
      if (!is.null(taskq)) {
         val <- c(val, qname)
		 ipcTaskQueueRelease(taskq)
	  }
  },
  error=function(e) {
    cat("failed to open task queue ", qname)
  }) 
  }
  val
}
# a rather dangerous function
rmSessions <- function(qnames=NULL, all.names=FALSE) {
  d <- character(0)
  if (!is.null(qnames)) {
    if (all.names) {
      stop('cannot specify both qnames and set all.names to TRUE')
    } else if (!is.character(qnames)) {
      stop('qnames must be a character variable')
    } else {
      # silently ignore zero length queue names
      qnames <- qnames[nzchar(qnames)]
      ibad <- which(is.na(match(qnames, QNAMES)))
      if (length(ibad) > 0) {
        goodstr <- paste(QNAMES, collapse=', ')
        warning('the only legal values for qnames are: ', goodstr)
        badstr <- paste(qnames[ibad], collapse=', ')
        stop('illegal qnames specified: ', badstr)
      } else if (length(qnames) == 0) {
        warning('empty qnames specified: ignoring')
      } else {
        cat(sprintf('attempting to delete qnames: %s\n',
                    paste(qnames, collapse=', ')))
        s <- sapply(qnames, rmSession)
        d <- qnames[s]   # deleted
        e <- qnames[!s]  # not deleted

        if (length(d) > 0)
          cat(sprintf('successfully deleted queues: %s\n',
                      paste(d, collapse=', ')))

        if (length(e) > 0)
          cat(sprintf('unable to delete queues: %s\n',
                      paste(e, collapse=', ')))
      }
    }
  } else {
    if (all.names) {
      cat(sprintf('attempting to delete qnames: %s\n',
                  paste(QNAMES, collapse=', ')))
      s <- sapply(QNAMES, rmSession)
      d <- QNAMES[s]   # deleted

      if (length(d) > 0) {
        cat(sprintf('successfully deleted queues: %s\n',
                    paste(QNAMES[s], collapse=', ')))
      } else {
        cat('no queues were deleted\n')
	  }
    } else {
      warning('rmSessions does nothing unless qnames is specified',
              ' or all.names is TRUE')
    }
  }
  invisible(NULL)
}

rmSession <- function(qname) {
  if (!is.character(qname) || length(qname) != 1)
    stop('qname must be a character variable of length one')

  tryCatch({
    taskq <- ipcTaskQueueOpen(qname)
    if (!is.null(taskq))
      destroyTaskQueue(taskq)
    else
      FALSE
  },
  error=function(e) {
    FALSE
  })
}

destroyTaskQueue <- function(taskq, delay=5) {
  if (is.null(taskq))
    stop('taskq must be a task queue object')

  ipcTaskQueueShutdown(taskq)
  Sys.sleep(delay)
  ipcTaskQueueDestroy(taskq)
}
