\name{revoIPC}
\alias{ipcCanFork}
\alias{ipcFork}
\alias{ipcMsgQueueCreate}
\alias{ipcMsgQueueDestroy}
\alias{ipcMsgQueueGetCapacity}
\alias{ipcMsgQueueGetCount}
\alias{ipcMsgQueueGetMaxSize}
\alias{ipcMsgQueueOpen}
\alias{ipcMsgQueueRelease}
\alias{ipcMsgReceive}
\alias{ipcMsgReceiveTry}
\alias{ipcMsgSend}
\alias{ipcMsgSendTry}
\alias{ipcMutexCreate}
\alias{ipcMutexDestroy}
\alias{ipcMutexLock}
\alias{ipcMutexLockTry}
\alias{ipcMutexOpen}
\alias{ipcMutexRelease}
\alias{ipcMutexUnlock}
\alias{ipcSemaphoreCreate}
\alias{ipcSemaphoreDestroy}
\alias{ipcSemaphoreOpen}
\alias{ipcSemaphorePost}
\alias{ipcSemaphoreRelease}
\alias{ipcSemaphoreWait}
\alias{ipcSemaphoreWaitTry}
\alias{ipcTaskCheckResult}
\alias{ipcTaskFetch}
\alias{ipcTaskQueueCreate}
\alias{ipcTaskQueueDestroy}
\alias{ipcTaskQueueOpen}
\alias{ipcTaskQueueRelease}
\alias{ipcTaskQueueShutdown}
\alias{ipcTaskReturnResult}
\alias{ipcTaskSetEnvironment}
\alias{ipcTaskStore}
\alias{ipcTaskWaitResult}
\alias{ipcMutexDestroy,mutex-method}
\alias{ipcMutexDestroy,recMutex-method}
\alias{ipcMutexLock,mutex-method}
\alias{ipcMutexLock,recMutex-method}
\alias{ipcMutexLockTry,mutex-method}
\alias{ipcMutexLockTry,recMutex-method}
\alias{ipcMutexRelease,mutex-method}
\alias{ipcMutexRelease,recMutex-method}
\alias{ipcMutexUnlock,mutex-method}
\alias{ipcMutexUnlock,recMutex-method}
\title{Interprocess Communication Tools}
\description{
The \pkg{revoIPC} package is a set of low-level interprocess communication tools
used by the \pkg{doSMP} package. None of the functions is intended to be called 
by end users.
}
\keyword{utilities}

