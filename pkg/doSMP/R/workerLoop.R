workerLoop <- function(qname, rank, verbose=FALSE, out=stdout()) {
  # initialization
  if (verbose) {
    cat(sprintf('opening queue %s\n', qname), file=out)
    flush(out)
  }

  taskq <- ipcTaskQueueOpen(qname)
  if (is.null(taskq))
    stop(sprintf('error opening queue %s', qname))

  # make sure we release the task queue before returning
  on.exit(ipcTaskQueueRelease(taskq))

  envir <- NULL
  expr <- NULL
  initError <- simpleError('not initialized')

  initializer <- function() {
    # current job initialization
    if (!is.null(envir)) {
      # call the specified initEnvir function
      initEnvir <- get('.$foreachInitEnvir', pos=envir, inherits=FALSE)
      if (!is.null(initEnvir)) {
        if (verbose) {
          cat('calling user specified initEnvir function\n', file=out)
          flush(out)
        }
        if (length(formals(initEnvir)) > 0) {
          initArgs <- get('.$foreachInitArgs', pos=envir, inherits=FALSE)
          eval(as.call(c(as.name('.$foreachInitEnvir'),
                         list(envir), initArgs)), envir=envir)
        } else {
          eval(call('.$foreachInitEnvir'), envir=envir)
        }
      }
    }
  }

  finalizer <- function() {
    # previous job finalization
    if (!is.null(envir)) {
      # call the specified finalEnvir function
      finalEnvir <- get('.$foreachFinalEnvir', pos=envir, inherits=FALSE)
      if (!is.null(finalEnvir)) {
        if (verbose) {
          cat('calling user specified finalEnvir function\n', file=out)
          flush(out)
        }
        if (length(formals(finalEnvir)) > 0) {
          finalArgs <- get('.$foreachFinalArgs', pos=envir, inherits=FALSE)
          eval(as.call(c(as.name('.$foreachFinalEnvir'),
                         list(envir), finalArgs)), envir=envir)
        } else {
          eval(call('.$foreachFinalEnvir'), envir=envir)
        }
      }
    }
  }

  # task loop
  while (TRUE) {
    # get some tasks to evaluate
    if (verbose) {
      cat('waiting for task...\n', file=out)
      flush(out)
    }
    taskchunk <- ipcTaskFetch(taskq)

    if (is.null(taskchunk)) {
      # task queue has been shut down
      if (verbose) {
        cat('got a NULL task: breaking out of task loop\n', file=out)
        flush(out)
      }
      break
    } else if (is.null(taskchunk$data)) {
      # we got a poison pill, so return a NULL in reply
      if (verbose) {
        cat('got a poison pill: replying and breaking loop\n', file=out)
        flush(out)
      }
      ipcTaskReturnResult(taskq, taskchunk, NULL)
      break
    } else {
      ntasks <- taskchunk$data$ntasks
      taskid <- taskchunk$data$taskid

      if (verbose) {
        if (ntasks > 1) {
          tlist <- paste(seq(taskid, length=ntasks), collapse=' ')
          cat(sprintf('got tasks %s\n', tlist), file=out)
        } else {
          cat(sprintf('got task %s\n', taskid), file=out)
        }
        flush(out)
      }

      if (! is.null(taskchunk$env)) {
        if (verbose) {
          cat('got a new job environment\n', file=out)
          flush(out)
        }

        # finalize the previous job (if there was one)
        # but don't fail if an error occurs
        tryCatch({
          finalizer()
        },
        error=function(e) {
          cat(sprintf('an error occurred calling finalEnvir function: %s\n',
                      conditionMessage(e)), file=out)
          flush(out)
        })

        # job specific initialization
        envir <- taskchunk$env
        parent.env(envir) <- globalenv()
        expr <- get('.$foreachExpr', pos=envir, inherits=FALSE)
        packages <- get('.$foreachPackages', pos=envir, inherits=FALSE)

        initError <- tryCatch({
          # load the specified packages
          if (!is.null(packages)) {
            for (p in packages)
              require(p, character.only=TRUE, quietly=TRUE)
          }

          # initialize this job
          initializer()

          NULL
        },
        error=function(e) {
          cat('worker: error initializing:\n', file=out)
          print(e)
          e
        })
      }

      resultset <- if (is.null(initError)) {
        anames <- names(taskchunk$data$argset[[1]])

        FUN <- function(args) {
          # assign the value of the "varying arguments" in the environment
          lapply(anames, function(n) {
            assign(n, args[[n]], pos=envir)
            NULL
          })

          # evalute the task
          tryCatch({
            withCallingHandlers({
              eval(expr, envir=envir)
            },
            error=function(e) {
              e$tb <- sys.calls()
              signalCondition(e)
            })
          },
          error=function(e) {
            # print error message
            cat(sprintf('an error occurred evaluating task: %s\n',
                        conditionMessage(e)), file=out)

            # print current task
            cat('task:\n', file=out)
            print(expr)

            # the following sequence is used to extract the
            # portion of the traceback that is relevant to the
            # user.
            idx <- rev(seq(11, length=length(e$tb)-12))
            if (length(idx) > 0) {
              cat('traceback:\n', file=out)
              for (i in idx) {
                cat('=> ', file=out)
                print(e$tb[[i]])
              }
            }
            e
          })
        }
        lapply(taskchunk$data$argset, FUN)
      } else {
        lapply(taskchunk$data$argset, function(a) initError)
      }

      # return the results of task evaluation
      if (verbose) {
        cat('returning task results\n', file=out)
        flush(out)
      }
      resultchunk <- list(resultset=resultset, ntasks=ntasks, taskid=taskid)
      ipcTaskReturnResult(taskq, taskchunk, resultchunk)
    }
  }

  # finalize the previous job (if there was one)
  # but don't fail if an error occurs
  tryCatch({
    finalizer()
  },
  error=function(e) {
    cat(sprintf('an error occurred calling finalEnvir function: %s\n',
                conditionMessage(e)), file=out)
    flush(out)
  })

  if (verbose) {
    cat('returning from workerLoop\n', file=out)
    flush(out)
  }

  invisible(NULL)
}
