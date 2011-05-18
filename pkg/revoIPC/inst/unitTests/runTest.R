library(RUnit)
library(revoIPC)

force.create.sem <- function(name, count) {
    sem <- tryCatch(ipcSemaphoreCreate(name, count), error=function(e) NULL)
    if (is.null(sem)) {
        sem <- ipcSemaphoreOpen(name)
        ipcSemaphoreDestroy(sem)
        sem <- ipcSemaphoreCreate(name, count)
    }
    sem
}

force.create.mutex <- function(name, rec) {
    mut <- tryCatch(ipcMutexCreate(name, rec), error=function(e) NULL)
    if (is.null(mut)) {
        mut <- ipcMutexOpen(name, rec)
        ipcMutexDestroy(mut)
        mut <- ipcMutexCreate(name, rec)
    }
    mut
}

force.create.mq <- function(name, maxmsg, maxsize) {
    mq <- tryCatch(ipcMsgQueueCreate(name, maxmsg, maxsize), error=function(e) NULL)
    if (is.null(mq)) {
        mq <- ipcMsgQueueOpen(name)
        ipcMsgQueueDestroy(mq)
        mq <- ipcMsgQueueCreate(name, maxmsg, maxsize)
    }
    mq
}

#####################################################
## Semaphores
#####################################################

test.sem.allnonblock <- function() {
    master <- force.create.sem('test_semaphore', 3)
    checkTrue(! is.null(master))
    worker <- ipcSemaphoreOpen('test_semaphore')
    checkTrue(ipcSemaphoreWait(worker))
    checkTrue(ipcSemaphoreWaitTry(worker))
    checkTrue(ipcSemaphoreWait(worker))
    checkTrue(! ipcSemaphoreWaitTry(worker))
    checkTrue(ipcSemaphorePost(master))
    checkTrue(ipcSemaphoreWaitTry(worker))
    checkTrue(! ipcSemaphoreWaitTry(worker))
    checkTrue(ipcSemaphorePost(master))
    checkTrue(ipcSemaphorePost(master))
    checkTrue(ipcSemaphoreWaitTry(worker))
    checkTrue(ipcSemaphoreWaitTry(worker))
    checkTrue(! ipcSemaphoreWaitTry(worker))
    checkTrue(! ipcSemaphoreWaitTry(master))
    checkTrue(ipcSemaphorePost(worker))
    checkTrue(ipcSemaphorePost(worker))
    checkTrue(ipcSemaphorePost(worker))
    checkTrue(ipcSemaphoreWaitTry(master))
    checkTrue(ipcSemaphoreWaitTry(master))
    checkTrue(ipcSemaphoreWaitTry(master))
    checkTrue(! ipcSemaphoreWaitTry(master))
    ipcSemaphoreRelease(worker)
    ipcSemaphoreDestroy(master)
}

test.sem.stale <- function() {
    sem <- force.create.sem('test_semaphore', 3)
    options(show.error.messages=FALSE)
    checkException(ipcSemaphoreCreate('test_semaphore', 3))
    checkException(ipcSemaphoreCreate('test_semaphore', 0))
    options(show.error.messages=TRUE)
    ipcSemaphoreDestroy(sem)
    options(show.error.messages=FALSE)
    ipcSemaphoreRelease(sem)           # Not currently an error, but shouldn't crash
    ipcSemaphoreDestroy(sem)           # Not currently an error, but shouldn't crash
    checkException(ipcSemaphorePost(sem))
    checkException(ipcSemaphoreWait(sem))
    checkException(ipcSemaphoreWaitTry(sem))
    checkException(ipcSemaphoreOpen('test_semaphore'))
    options(show.error.messages=TRUE)
}

## It's not clear that this can be a useful test.  Posix semaphores can be
## "unlinked" while handles are still open, to be deleted when all references
## are closed.  Windows semaphores almost certainly cannot be deleted until all
## references are closed.
##
# test.sem.destroyed <- function() {
#     sem0 <- force.create.sem('test_semaphore', 3)
#     sem1 <- ipcSemaphoreOpen('test_semaphore')
#     ipcSemaphoreDestroy(sem0)
#     options(show.error.messages=FALSE)
#     ipcSemaphorePost(sem1)
#     ipcSemaphoreWait(sem1)
#     ipcSemaphoreWaitTry(sem1)
#     ipcSemaphoreDestroy(sem1)
#     options(show.error.messages=TRUE)
# }

#####################################################
## Mutexes
#####################################################

test.mutex.allnonblock <- function() {
    master <- force.create.mutex('test_mutex', FALSE)
    checkTrue(! is.null(master))
    worker <- ipcMutexOpen('test_mutex', FALSE)
    checkTrue(ipcMutexLock(worker))
    checkTrue(! ipcMutexLockTry(worker))
    checkTrue(! ipcMutexLockTry(master))
    checkTrue(ipcMutexUnlock(worker))
    checkTrue(ipcMutexLockTry(master))
    checkTrue(! ipcMutexLockTry(worker))
    checkTrue(! ipcMutexLockTry(master))
    checkTrue(ipcMutexUnlock(master))
    ipcMutexRelease(worker)
    ipcMutexDestroy(master)
}

test.rmutex.allnonblock <- function() {
    master <- force.create.mutex('test_rmutex', TRUE)
    checkTrue(! is.null(master))
    worker <- ipcMutexOpen('test_rmutex', TRUE)
    checkTrue(ipcMutexLock(worker))
    checkTrue(ipcMutexLockTry(worker))
    ## SIGH...  if pid(master) == pid(worker), 'master' and 'worker' are
    ## equivalent on Posix.
    # checkTrue(! ipcMutexLockTry(master))
    checkTrue(ipcMutexUnlock(worker))
    # checkTrue(! ipcMutexLockTry(master))
    checkTrue(ipcMutexUnlock(worker))
    checkTrue(ipcMutexLockTry(master))
    checkTrue(ipcMutexLock(master))
    checkTrue(ipcMutexLock(master))
    # checkTrue(! ipcMutexLockTry(worker))
    checkTrue(ipcMutexUnlock(master))
    # checkTrue(! ipcMutexLockTry(worker))
    checkTrue(ipcMutexUnlock(master))
    # checkTrue(! ipcMutexLockTry(worker))
    checkTrue(ipcMutexUnlock(master))
    checkTrue(ipcMutexLockTry(worker))
    checkTrue(ipcMutexUnlock(worker))
    ipcMutexRelease(worker)
    ipcMutexDestroy(master)
}

test.mutex.stale <- function() {
    master <- force.create.mutex('test_mutex', FALSE)
    checkTrue(! is.null(master))
    ipcMutexDestroy(master)
    options(show.error.messages=FALSE)
    checkException(ipcMutexLock(master))
    checkException(ipcMutexLockTry(master))
    checkException(ipcMutexUnlock(master))
    options(show.error.messages=TRUE)
    ipcMutexRelease(master)
    ipcMutexDestroy(master)
}

test.rmutex.stale <- function() {
    master <- force.create.mutex('test_rmutex', TRUE)
    checkTrue(! is.null(master))
    ipcMutexDestroy(master)
    options(show.error.messages=FALSE)
    checkException(ipcMutexLock(master))
    checkException(ipcMutexLockTry(master))
    checkException(ipcMutexUnlock(master))
    options(show.error.messages=TRUE)
    ipcMutexRelease(master)
    ipcMutexDestroy(master)
}


#####################################################
## Message Queues
#####################################################

test.mq.allnonblock <- function() {
    mq <- force.create.mq('test_mq', 16, 100)
    mqR <- ipcMsgQueueOpen('test_mq')
    checkEquals(16, ipcMsgQueueGetCapacity(mq))
    checkEquals(0, ipcMsgQueueGetCount(mq))
    checkTrue(ipcMsgQueueGetMaxSize(mq) >= 100)

    # Fill the queue
    for (i in 1:16) {
        checkTrue(ipcMsgSendTry(mq, i))
    }
    checkEquals(16, ipcMsgQueueGetCount(mq))

    # Next send should fail
    checkTrue(! ipcMsgSendTry(mq, 100))

    # First 16 receives should match first 16 sends
    for (i in 1:16) {
        checkEquals(i, ipcMsgReceiveTry(mqR))
    }
    checkEquals(0, ipcMsgQueueGetCount(mq))

    # Next receive should fail
    checkTrue(is.null(ipcMsgReceiveTry(mqR)))

    # Fill the queue again, blocking ops
    for (i in 17:32) {
        checkTrue(ipcMsgSend(mq, i))
    }
    checkEquals(16, ipcMsgQueueGetCount(mq))

    # Next send should fail
    checkTrue(! ipcMsgSendTry(mq, 100))

    # Next 16 receives should match second 16 sends
    for (i in 17:32) {
        checkEquals(i, ipcMsgReceive(mqR))
    }
    checkEquals(0, ipcMsgQueueGetCount(mq))

    # Next receive should fail
    checkTrue(is.null(ipcMsgReceiveTry(mqR)))

    ipcMsgQueueRelease(mqR)
    ipcMsgQueueDestroy(mq)
}

test.mq.stale <- function() {
    mq <- force.create.mq('test_mq', 16, 100)
    ipcMsgQueueDestroy(mq)
    options(show.error.messages=FALSE)
    checkException(ipcMsgSend(mq, 100))
    checkException(ipcMsgSendTry(mq, 100))
    checkException(ipcMsgReceive(mq, 100))
    checkException(ipcMsgReceiveTry(mq, 100))
    checkException(ipcMsgQueueGetCapacity(mq))
    checkException(ipcMsgQueueGetCount(mq))
    checkException(ipcMsgQueueGetMaxSize(mq))
    options(show.error.messages=TRUE)
}

