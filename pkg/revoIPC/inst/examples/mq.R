library(revoIPC)

mq_master <- function(name, limit) {
    mq <- ipcMsgQueueCreate(name, 30, 200)
    sem <- ipcSemaphoreCreate(paste(name, '_sem', sep=''), 0)
    for (i in 1:limit) {
        ipcMsgSend(mq, i)
    }
    ipcMsgSend(mq, -1)
    ipcSemaphoreWait(sem)
    Sys.sleep(5)
    ipcMsgQueueDestroy(mq)
    ipcSemaphoreDestroy(sem)
}

mq_slave <- function(name, delay, id='slave') {
    mq <- ipcMsgQueueOpen(name)
    sem <- ipcSemaphoreOpen(paste(name, '_sem', sep=''))
    while (TRUE) {
        msg <- ipcMsgReceive(mq)
        cat(id, msg, '\n')
        Sys.sleep(delay)
        if (msg == -1) {
            ipcMsgSend(mq, -1)
            break
        }
    }
    ipcMsgQueueRelease(mq)
    ipcSemaphorePost(sem)
}
