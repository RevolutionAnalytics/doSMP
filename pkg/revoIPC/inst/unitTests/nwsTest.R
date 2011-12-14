if ("nws" %in% row.names(installed.packages()) && compareVersion(installed.packages()["nws", "Version"],"2.0") >= 0) {
library(RUnit)
library(revoIPC)
library(nws)

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

helper.sem.allblock <- function() {
    library(revoIPC)
    # publishEvent <- function(ev) { nwsStore(SleighUserNws, 'events', ev)
    # publishEvent('begin')
    sem0 <- ipcSemaphoreOpen('test_semaphore0')
    sem1 <- ipcSemaphoreOpen('test_semaphore1')
    # publishEvent('semaphores')
    ipcSemaphoreWait(sem0)
    # publishEvent('gotsem0')
    ipcSemaphorePost(sem1)
    # publishEvent('postsem1')
    ipcSemaphoreRelease(sem0)
    ipcSemaphoreRelease(sem1)
    # publishEvent('end')
}
test.sem.allblock <- function() {
    sem0 <- force.create.sem('test_semaphore0', 0)
    sem1 <- force.create.sem('test_semaphore1', 0)

    # Use sleigh to start 3 workers running "helper"
    s <- sleigh()
    wsHandle <- userNws(s)
    sp <- eachWorker(s, helper.sem.allblock, eo=list(closure=FALSE, blocking=FALSE))
    # printEvents <- function() {
    #     while (TRUE) {
    #         val <- nwsFetchTry(wsHandle, 'events')
    #         if (is.null(val)) break
    #         else cat('Event: ', val, '\n')
    #     }
    # }

    # Initially, we cannot wait on sem1
    checkTrue(! ipcSemaphoreWaitTry(sem0))
    checkTrue(! ipcSemaphoreWaitTry(sem1))
    # printEvents()

    # Wake one worker and wait for response
    ipcSemaphorePost(sem0)
    ipcSemaphoreWait(sem1)
    checkTrue(! ipcSemaphoreWaitTry(sem0))
    checkTrue(! ipcSemaphoreWaitTry(sem1))
    # printEvents()

    # Wake second worker and wait for response
    ipcSemaphorePost(sem0)
    ipcSemaphoreWait(sem1)
    checkTrue(! ipcSemaphoreWaitTry(sem0))
    checkTrue(! ipcSemaphoreWaitTry(sem1))
    # printEvents()

    # Wake third worker and wait for response
    ipcSemaphorePost(sem0)
    ipcSemaphoreWait(sem1)
    checkTrue(! ipcSemaphoreWaitTry(sem0))
    checkTrue(! ipcSemaphoreWaitTry(sem1))
    # printEvents()

    # Sleigh workers should be done now.
    waitSleigh(sp)
    # printEvents()

    # Clean up
    close(s)
    ipcSemaphoreDestroy(sem0)
    ipcSemaphoreDestroy(sem1)
}

helper.mutex.allblock <- function() {
    library(revoIPC)
    # publishEvent <- function(ev) { nwsStore(SleighUserNws, 'events', ev)
    # publishEvent('begin')
    mtx <- ipcMutexOpen('test_mutex', FALSE)
    for (i in 1:250) {
        ipcMutexLock(mtx)
        value <- nwsFind(SleighUserNws, 'counter')
        nwsStore(SleighUserNws, 'counter', value + 1)
        ipcMutexUnlock(mtx)
    }
    ipcMutexRelease(mtx)
    # publishEvent('end')
}

test.mutex.allblock <- function() {
    mtx <- force.create.mutex('test_mutex', FALSE)

    # Use sleigh to start 3 workers running "helper"
    s <- sleigh()
    wsHandle <- userNws(s)
    nwsDeclare(wsHandle, 'counter', 'single')
    nwsStore(wsHandle, 'counter', 0)
    sp <- eachWorker(s, helper.mutex.allblock, eo=list(closure=FALSE, blocking=FALSE))
    # # printEvents <- function() {
    # #     while (TRUE) {
    # #         val <- nwsFetchTry(wsHandle, 'events')
    # #         if (is.null(val)) break
    # #         else cat('Event: ', val, '\n')
    # #     }
    # # }

    # Sleigh workers should be done now.
    waitSleigh(sp)
    checkEquals(nwsFetch(wsHandle, 'counter'), 750)
    # printEvents()

    # Clean up
    close(s)
    ipcMutexDestroy(mtx)
}

helper.rmutex.allblock <- function() {
    library(revoIPC)

    # publishEvent <- function(ev) { nwsStore(SleighUserNws, 'events', ev)
    # publishEvent('begin')
    mtx <- ipcMutexOpen('test_mutex', TRUE)
    for (i in 1:250) {
        ipcMutexLock(mtx)
        ipcMutexLock(mtx)   # The mutex so nice, they locked it twice!
        value <- nwsFind(SleighUserNws, 'counter')
        nwsStore(SleighUserNws, 'counter', value + 1)
        ipcMutexUnlock(mtx)
        ipcMutexUnlock(mtx)
    }
    ipcMutexRelease(mtx)
    # publishEvent('end')
}

test.rmutex.allblock <- function() {
    mtx <- force.create.mutex('test_mutex', TRUE)

    # Use sleigh to start 3 workers running "helper"
    s <- sleigh()
    wsHandle <- userNws(s)
    nwsDeclare(wsHandle, 'counter', 'single')
    nwsStore(wsHandle, 'counter', 0)
    sp <- eachWorker(s, helper.rmutex.allblock, eo=list(closure=FALSE, blocking=FALSE))
    # # printEvents <- function() {
    # #     while (TRUE) {
    # #         val <- nwsFetchTry(wsHandle, 'events')
    # #         if (is.null(val)) break
    # #         else cat('Event: ', val, '\n')
    # #     }
    # # }

    # Sleigh workers should be done now.
    waitSleigh(sp)
    value <- nwsFetch(wsHandle, 'counter')
    checkEquals(value, 750)
    # printEvents()

    # Clean up
    close(s)
    ipcMutexDestroy(mtx)
}

helper.mq.allblock <- function() {
    library(revoIPC)
    # publishEvent <- function(ev) { nwsStore(SleighUserNws, 'events', ev) }
    # publishEvent('begin')
    mq1 <- ipcMsgQueueOpen('test_mq1')
    mq2 <- ipcMsgQueueOpen('test_mq2')
    while (TRUE) {
        # publishEvent('waiting')
        val <- ipcMsgReceive(mq1)
        # publishEvent(val)
        if (val == -1) break
        # publishEvent('replying')
        ipcMsgSend(mq2, val + 100)
    }
    ipcMsgQueueRelease(mq1)
    ipcMsgQueueRelease(mq2)
    # publishEvent('end')
}

test.mq.block <- function() {
    mq1 <- force.create.mq('test_mq1', 16, 100)
    mq2 <- force.create.mq('test_mq2', 16, 100)

    # Use sleigh to start 3 workers running "helper"
    s <- sleigh()
    wsHandle <- userNws(s)
    sp <- eachWorker(s, helper.mq.allblock, eo=list(closure=FALSE, blocking=FALSE))
    # printEvents <- function() {
    #     while (TRUE) {
    #         val <- nwsFetchTry(wsHandle, 'events')
    #         if (is.null(val)) { print('End of events.'); break }
    #         else cat('Event: ', val, '\n')
    #     }
    # }

    # print('Number 1')
    # printEvents()

    # Send the first batch
    for (i in 1:16) {
        ipcMsgSend(mq1, i)
    }

    # print('Number 2')
    # printEvents()

    # Drain them
    sum <- 0
    for (i in 1:16) {
        sum <- sum + ipcMsgReceive(mq2)
    }
    checkEquals(sum, 1736)

    # print('Number 3')
    # printEvents()

    # Kill a worker
    ipcMsgSend(mq1, -1)

    # print('Number 4')

    # Send the second batch
    for (i in 17:32) {
        ipcMsgSend(mq1, i)
    }

    # print('Number 5')

    # Drain them
    sum <- 0
    for (i in 1:16) {
        sum <- sum + ipcMsgReceive(mq2)
    }
    checkEquals(sum, 1992)

    # print('Number 6')

    # Kill a worker
    ipcMsgSend(mq1, -1)

    # print('Number 7')

    # Send the third batch
    for (i in 33:48) {
        ipcMsgSend(mq1, i)
    }

    # print('Number 8')

    # Drain them
    sum <- 0
    for (i in 1:16) {
        sum <- sum + ipcMsgReceive(mq2)
    }
    checkEquals(sum, 2248)

    # print('Number 9')

    # Kill the last worker
    ipcMsgSend(mq1, -1)

    # print('Number 10')

    # Sleigh workers should be done now.
    waitSleigh(sp)
    # printEvents()

    # print('Number 11')
    # Clean up
    close(s)
    ipcMsgQueueDestroy(mq1)
    ipcMsgQueueDestroy(mq2)
}
}
