library(revoIPC)

tq_master <- function(name, tmpdir, limit) {
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

tq_slave <- function(name, delay) {
    q <- ipcTaskQueueOpen(name)
    while (TRUE) {
        task <- ipcTaskFetch(q)
        if (is.null(task)) break
        else {
            Sys.sleep(delay)
            if (! is.null(task$env)) cat('slaveEnv', task$env, '\n')
            res <- task$data * task$data
            ipcTaskReturnResult(q, task, res)
        }
    }
    ipcTaskQueueRelease(q)
}
