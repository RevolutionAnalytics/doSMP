w <- NULL

test00 <- function() {
  verbose <- as.logical(Sys.getenv('FOREACH_VERBOSE', 'FALSE'))
  w <<- startWorkers(verbose=verbose)
  registerDoSMP(w)
}

test01 <- function() {
  x <- list(1:3, 1:9, 1:19)
  cs <- 1:20

  for (chunkSize in cs) {
    smpopts <- list(chunkSize=chunkSize)
    for (y in x) {
      actual <- foreach(i=y, .options.smp=smpopts) %dopar% i
      checkEquals(actual, as.list(y))
      actual <- foreach(i=y, .combine='c', .options.smp=smpopts) %do% i
      checkEquals(actual, y)
    }
  }
}

test99 <- function() {
  cat('shutting down SMP workers...\n')
  stopWorkers(w)
  cat('shutdown complete\n')
}
