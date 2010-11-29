# Private functions
q.getId         <- function(q, t)   .Call("getId", q, t)
q.getTaskEnv    <- function(q, t)   {
    e <- .Call("getTaskEnv", q, t)
    if (is.null(e)) NULL
    else unserialize(e)
}
q.getTaskData   <- function(q, t)   unserialize(.Call("getTaskData", q, t))
q.getResultData <- function(q, t)   unserialize(.Call("getResultData", q, t))
q.discardResult <- function(q, t)   .Call("discardResult", q, t)
mutex.create      <- function(name)         .Call("mutex_create", name)
mutex.open        <- function(name)         .Call("mutex_open", name)
mutex.release     <- function(mutex)        .Call("mutex_release", mutex)
mutex.destroy     <- function(name)         .Call("mutex_destroy", name)
mutex.lock        <- function(mutex)        .Call("mutex_lock", mutex)
mutex.lockTry     <- function(mutex)        .Call("mutex_lockTry", mutex)
mutex.unlock      <- function(mutex)        .Call("mutex_unlock", mutex)
rec.mutex.create  <- function(name)         .Call("rmutex_create", name)
rec.mutex.open    <- function(name)         .Call("rmutex_open", name)
rec.mutex.release <- function(mutex)        .Call("rmutex_release", mutex)
rec.mutex.destroy <- function(name)         .Call("rmutex_destroy", name)
rec.mutex.lock    <- function(mutex)        .Call("rmutex_lock", mutex)
rec.mutex.lockTry <- function(mutex)        .Call("rmutex_lockTry", mutex)
rec.mutex.unlock  <- function(mutex)        .Call("rmutex_unlock", mutex)

####
## Public interface

# Generic functions
ipcFork          <- function()       .Call("fork")
ipcCanFork       <- function()       .Call("canFork")

# Task Queue
ipcTaskQueueCreate   <- function(name, tmpdir, memsize=NULL, maxtasks=NULL, filethresh=NULL) .Call("create", name, tmpdir, memsize, maxtasks, filethresh)
ipcTaskQueueOpen     <- function(name)   .Call("open", name)
ipcTaskQueueShutdown <- function(q)      .Call("shutdown", q)
ipcTaskQueueRelease  <- function(q)      .Call("release", q)
ipcTaskQueueDestroy  <- function(q)      .Call("destroy", q)
ipcTaskStore         <- function(q, t)   .Call("enqueue", q, serialize(t, NULL))
ipcTaskFetch         <- function(q) {
    t <- .Call("dequeue", q)
    if (is.null(t)) NULL
    else list(task=t, data=q.getTaskData(q, t), env=q.getTaskEnv(q, t))
}
ipcTaskReturnResult  <- function(q, t, res) .Call("returnResult", q, t$task, serialize(res, NULL))
ipcTaskCheckResult   <- function(q) {
    r <- .Call("checkResult", q)
    if (is.null(r)) NULL
    else {
        l <- list(id=q.getId(q, r), data=q.getResultData(q, r))
        q.discardResult(q, r)
        l
    }
}
ipcTaskWaitResult <- function(q) {
    r <- .Call("waitResult", q)
    if (is.null(r)) NULL
    else {
        l <- list(id=q.getId(q, r), data=q.getResultData(q, r))
        q.discardResult(q, r)
        l
    }
}

ipcTaskSetEnvironment <- function(q, t)  .Call("setEnvironment", q, serialize(t, NULL))

# Semaphore
ipcSemaphoreCreate  <- function(name, init=0) {
    s <- .Call("sem_create", name, init)
    list(name=name, s=s)
}
ipcSemaphoreOpen    <- function(name) {
    s <- .Call("sem_open", name)
    list(name=name, s=s)
}
ipcSemaphoreRelease <- function(sem) {
    .Call("sem_release", sem$s)
    sem$s <- NULL
}
ipcSemaphoreDestroy <- function(sem) {
    ipcSemaphoreRelease(sem)
    .Call("sem_destroy", sem$name)
}
ipcSemaphoreWait    <- function(sem) {
    .Call("sem_wait", sem$s)
}
ipcSemaphoreWaitTry <- function(sem) {
    .Call("sem_waitTry", sem$s)
}
ipcSemaphorePost    <- function(sem) {
    .Call("sem_post", sem$s)
}

# Mutex
normalMutex <- setClass('mutex', representation(mutex='externalptr', name='character'))
recursiveMutex <- setClass('recMutex', representation(mutex='externalptr', name='character'))

ipcMutexCreate <- function(name, recursive=FALSE) {
    if (recursive) {
        m <- rec.mutex.create(name)
        mc <- recursiveMutex
    }
    else {
        m <- mutex.create(name)
        mc <- normalMutex
    }
    if (is.null(m)) NULL
    else
    {
        theMutex <- new(mc)
        theMutex@mutex <- m
        theMutex@name <- name
        theMutex
    }
}

ipcMutexOpen <- function(name, recursive=FALSE) {
    if (recursive) {
        m <- rec.mutex.open(name)
        mc <- recursiveMutex
    }
    else {
        m <- mutex.open(name)
        mc <- normalMutex
    }
    if (is.null(m)) NULL
    else
    {
        theMutex <- new(mc)
        theMutex@mutex <- m
        theMutex@name <- name
        theMutex
    }
}

ipcMutexLock <- function(mutex) logical()
setGeneric('ipcMutexLock')
setMethod('ipcMutexLock', 'mutex', function(mutex) mutex.lock(mutex@mutex))
setMethod('ipcMutexLock', 'recMutex', function(mutex) rec.mutex.lock(mutex@mutex))

ipcMutexLockTry <- function(mutex) logical()
setGeneric('ipcMutexLockTry')
setMethod('ipcMutexLockTry', 'mutex', function(mutex) mutex.lockTry(mutex@mutex))
setMethod('ipcMutexLockTry', 'recMutex', function(mutex) rec.mutex.lockTry(mutex@mutex))

ipcMutexUnlock <- function(mutex) logical()
setGeneric('ipcMutexUnlock')
setMethod('ipcMutexUnlock', 'mutex', function(mutex) mutex.unlock(mutex@mutex))
setMethod('ipcMutexUnlock', 'recMutex', function(mutex) rec.mutex.unlock(mutex@mutex))

# XXX: Perhaps a good idea to NULL these @mutex objects?
ipcMutexRelease <- function(mutex) logical()
setGeneric('ipcMutexRelease')
setMethod('ipcMutexRelease', 'mutex',
    function(mutex) {
        mutex.release(mutex@mutex)
    })
setMethod('ipcMutexRelease', 'recMutex',
    function(mutex) {
        rec.mutex.release(mutex@mutex)
    })

ipcMutexDestroy <- function(mutex) logical()
setGeneric('ipcMutexDestroy')
setMethod('ipcMutexDestroy', 'mutex',
    function(mutex) {
        ipcMutexRelease(mutex)
        mutex.destroy(mutex@name)
        TRUE
    })
setMethod('ipcMutexDestroy', 'recMutex',
    function(mutex) {
        ipcMutexRelease(mutex)
        rec.mutex.destroy(mutex@name)
        TRUE
    })

# Message Queues
ipcMsgQueueCreate  <- function(name, maxmsg, maxsize) {
    mq <- .Call("mq_create", name, maxmsg, maxsize)
    list(name=name, mq=mq)
}
ipcMsgQueueOpen    <- function(name) {
    mq <- .Call("mq_open", name)
    list(name=name, mq=mq)
}
ipcMsgQueueRelease <- function(mq) {
    .Call("mq_release", mq$mq)
    mq$mq <- NULL
}
ipcMsgQueueDestroy <- function(mq) {
    ipcMsgQueueRelease(mq)
    .Call("mq_destroy", mq$name)
}
ipcMsgSend         <- function(mq, msg, prio=0) .Call("mq_send", mq$mq, serialize(msg, NULL), prio)
ipcMsgSendTry      <- function(mq, msg, prio=0) .Call("mq_sendTry", mq$mq, serialize(msg, NULL), prio)
ipcMsgReceive      <- function(mq) {
    res <- .Call("mq_receive", mq$mq);
    if (is.null(res)) res else unserialize(res)
}
ipcMsgReceiveTry     <- function(mq) {
    res <- .Call("mq_receiveTry", mq$mq);
    if (is.null(res)) res else unserialize(res)
}
ipcMsgQueueGetCapacity    <- function(mq)           .Call("mq_getMaxMsgs", mq$mq)
ipcMsgQueueGetCount       <- function(mq)           .Call("mq_getNumMsgs", mq$mq)
ipcMsgQueueGetMaxSize     <- function(mq)           .Call("mq_getMaxSize", mq$mq)
