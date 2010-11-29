library(revoIPC)

mutex2_helper <- function(mutex, id, depth)
{
    if (depth <= 0) NULL
    else {
        ipcMutexLock(mutex)
        cat(id, depth, '\n')
        Sys.sleep(1)
        mutex2_helper(mutex, id, depth - 1)
        ipcMutexUnlock(mutex)
        Sys.sleep(1)
        NULL
    }
}

mutex2_master <- function(name, depth=3, count=3)
{
    mutex <- ipcMutexCreate(name, TRUE)
    for (i in 1:count) {
        mutex2_helper(mutex, 'master', depth)
    }
    ipcMutexDestroy(mutex)
}

mutex2_slave <- function(name, depth=3, count=3, id='slave')
{
    mutex <- ipcMutexOpen(name, TRUE)
    for (i in 1:count) {
        mutex2_helper(mutex, id, depth)
    }
    ipcMutexRelease(mutex)
}
