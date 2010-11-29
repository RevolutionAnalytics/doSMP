library(revoIPC)
library(nws)

tq_master <- function(name, limit=500, tmpdir=tempdir()) {
  q <- ipcTaskQueueCreate(name, tmpdir)
  highwm <- 192
  lowwm  <- 64
  curfill <- 0
  idx <- 0
  ipcTaskSetEnvironment(q, 'abcde')
  while (idx < limit  ||  curfill > 0) {
    if (curfill <= lowwm  &&  idx <= limit) {
      while (curfill <= highwm  &&  idx <= limit) {
        ipcTaskStore(q, idx)
        idx <- idx + 1
        curfill <- curfill + 1
      }
    } else {
      while (curfill > lowwm  ||  (idx >= limit && curfill > 0)) {
        resp <- ipcTaskWaitResult(q)
        print(resp$data)
        curfill <- curfill - 1
      }
    }
  }
  ipcTaskQueueShutdown(q)
  Sys.sleep(5)
  ipcTaskQueueDestroy(q)
}

tq_worker <- function(name, delay) {
  library(revoIPC)

  q <- ipcTaskQueueOpen(name)
  count <- 0
  while (TRUE) {
    task <- ipcTaskFetch(q)

    if (is.null(task))
      break

    Sys.sleep(delay)

    if (! is.null(task$env))
      cat('workerEnv', task$env, '\n')

    res <- c(task$data, SleighRank)
    ipcTaskReturnResult(q, task, res)
    count <- count + 1
  }
  ipcTaskQueueRelease(q)
  count
}

name <- 'bar'
delay <- 1
s <- sleigh(launch='local', workerCount=4, verbose=TRUE, logDir='.')
cat('starting the workers\n')
sp <- eachWorker(s, tq_worker, name, delay, eo=list(blocking=FALSE))
cat('running task queue example\n')
tq_master(name)
cat('waiting for workers to finish\n')
r <- waitSleigh(sp)
cat('finished\n')
