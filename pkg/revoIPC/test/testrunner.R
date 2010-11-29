library(RUnit)

res <- runTestFile('test.R')
printTextProtocol(res)

e <- getErrors(res)
if (e$nErr + e$nFail > 0) {
    quit('no', 1)
} else {
    quit('no', 0)
}
