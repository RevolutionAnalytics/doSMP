library(revoIPC)

mutex1_helper <- function(mutex, id)
{
    ipcMutexLock(mutex)
    print(id)
    Sys.sleep(1)
    ipcMutexUnlock(mutex)
    Sys.sleep(1)
}

mutex1_master <- function(name, count=5)
{
    mutex <- ipcMutexCreate(name, FALSE)
    for (i in 1:count) {
        mutex1_helper(mutex, 'master')
    }
    ipcMutexDestroy(mutex)
}

mutex1_slave <- function(name, count=5, id='slave')
{
    mutex <- ipcMutexOpen(name, FALSE)
    for (i in 1:count) {
        mutex1_helper(mutex, id)
    }
    ipcMutexRelease(mutex)
}
