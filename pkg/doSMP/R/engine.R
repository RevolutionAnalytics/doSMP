engine <- function(w, it, expr, envir, packages, options) {
  if (!inherits(w, 'workergroup'))
    stop('internal error: invalid worker group object')

  taskq <- w$taskq

  assign('.$foreachExpr', expr, pos=envir)
  assign('.$foreachPackages', packages, pos=envir)

  initEnvir <- options$initEnvir
  if (!is.null(initEnvir))
    environment(initEnvir) <- envir
  assign('.$foreachInitEnvir', initEnvir, pos=envir)
  assign('.$foreachInitArgs', options$initArgs, pos=envir)

  finalEnvir <- options$finalEnvir
  if (!is.null(finalEnvir))
    environment(finalEnvir) <- envir
  assign('.$foreachFinalEnvir', finalEnvir, pos=envir)
  assign('.$foreachFinalArgs', options$finalArgs, pos=envir)

  ipcTaskSetEnvironment(taskq, envir)

  HIGHWM <- 192L  # XXX use loadFactor
  LOWWM <- 64L
  taskInfo <- list(level=0L, nextTaskId=1L, hasMore=TRUE)

  chunkSize <- if (is.null(options$chunkSize)) 1L else options$chunkSize

  submitTasks <- function(highwm) {
    if (options$verbose)
      cat('submitting task chunk\n')

    level <- taskInfo$level
    nextTaskId <- taskInfo$nextTaskId
    hasMore <- taskInfo$hasMore

    # submit tasks up to the high water mark
    while (hasMore && level < highwm) {
      if (options$verbose)
        cat(sprintf('level is %d\n', level))
      ntasks <- 0L  # number of tasks in the task chunk that I'm building
      argset <- vector('list', length=chunkSize)
      tryCatch({
        while (ntasks < chunkSize) {
          args <- nextElem(it)
          if (options$verbose) {
            cat('next set of arguments:\n')
            print(args)
          }
          ntasks <- ntasks + 1L
          argset[[ntasks]] <- args  # args cannot be NULL
        }
      },
      error=function(e) {
        if (!identical(conditionMessage(e), 'StopIteration')) {
          msg <- paste('error calling iterator:', conditionMessage(e))
          stop(simpleError(msg, call=conditionCall(e)))
        }
        hasMore <<- FALSE
      })

      # check if we managed to get at least one task for the chunk
      if (ntasks == 0L)
        break

      # trim the size of argset if necessary
      if (ntasks < chunkSize)
        length(argset) <- ntasks

      # send the task chunk to the workers
      taskchunk <- list(argset=argset, ntasks=ntasks, taskid=nextTaskId)
      ipcTaskStore(taskq, taskchunk)
      if (options$verbose) {
        if (ntasks > 1)
          cat(sprintf('submitted tasks %d:%d\n',
                      nextTaskId, nextTaskId, + ntasks - 1))
        else
          cat(sprintf('submitted task %d\n', nextTaskId))
      }

      level <- level + 1L
      nextTaskId <- nextTaskId + ntasks
    }

    list(level=level, nextTaskId=nextTaskId, hasMore=hasMore)
  }

  processResults <- function(lowwm) {
    if (options$verbose)
      cat('processing results\n')

    level <- taskInfo$level

    while (level > lowwm) {
      # wait for a result
      resp <- ipcTaskWaitResult(taskq)
      ntasks <- resp$data$ntasks
      taskid <- resp$data$taskid
      if (options$verbose) {
        cat(sprintf('got chunk of %d result(s) starting at # %d\n',
                    ntasks, taskid))
      }

      for (i in seq(length=ntasks)) {
        tid <- taskid + i - 1L
        tryCatch({
          accumulate(it, resp$data$resultset[[i]], tid)
        },
        error=function(e) {
          cat('error calling combine function:\n')
          print(e)
        })
      }

      level <- level - 1L
    }

    list(level=level, nextTaskId=taskInfo$nextTaskId, hasMore=taskInfo$hasMore)
  }

  # alternately submit tasks and process results until
  # there are no more tasks
  while (taskInfo$hasMore) {
    if (options$verbose) {
      cat(sprintf('current task info: level %d; nextTaskId %d; hasMore %s\n',
                  taskInfo$level, taskInfo$nextTaskId, taskInfo$hasMore))
    }

    taskInfo <- if (taskInfo$level < HIGHWM)
      submitTasks(HIGHWM)
    else
      processResults(LOWWM)
  }

  if (options$verbose) {
    cat('all tasks have been submitted\n')
    cat(sprintf('current task info: level %d; base %d\n',
                taskInfo$level, taskInfo$base))
  }

  # finish processing all of the results
  processResults(0L)

  invisible(NULL)
}
