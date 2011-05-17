itimer <- function(it, time) {
  it <- iter(it)
  start <- proc.time()[[3]]

  nextEl <- function() {
    current <- proc.time()[[3]]
    if (current - start >= time)
      stop('StopIteration')

    nextElem(it)
  }

  obj <- list(nextElem=nextEl)
  class(obj) <- c('itimer', 'abstractiter', 'iter')
  obj
}

w <- NULL

test00 <- function() {
  verbose <- as.logical(Sys.getenv('FOREACH_VERBOSE', 'FALSE'))
  w <<- startWorkers(verbose=verbose)
  registerDoSMP(w)
}

# big results
test01 <- function() {
  n <- 1000        # size of inputs
  m <- 100000      # size of outputs
  time <- 10 * 60  # run for ten minutes
  it <- itimer(irnorm(n, mean=1000), time=time)
  r <- foreach(x=it, .combine='+') %dopar% rep(1, m)
  checkTrue(is.atomic(r))
  checkTrue(inherits(r, 'numeric'))
  checkTrue(length(r) == m)
  cat(sprintf('stressTest: ran %d iterations\n', r[1]))
}

# big tasks
test02 <- function() {
  n <- 100000      # size of inputs
  m <- 1000        # size of outputs
  time <- 10 * 60  # run for ten minutes
  it <- itimer(irnorm(n, mean=1000), time=time)
  r <- foreach(x=it, .combine='+') %dopar% rep(1, m)
  checkTrue(is.atomic(r))
  checkTrue(inherits(r, 'numeric'))
  checkTrue(length(r) == m)
  cat(sprintf('stressTest: ran %d iterations\n', r[1]))
}

# big tasks and big results
test03 <- function() {
  n <- 100000      # size of inputs
  m <- 100000      # size of outputs
  time <- 10 * 60  # run for ten minutes
  it <- itimer(irnorm(n, mean=1000), time=time)
  r <- foreach(x=it, .combine='+') %dopar% rep(1, m)
  checkTrue(is.atomic(r))
  checkTrue(inherits(r, 'numeric'))
  checkTrue(length(r) == m)
  cat(sprintf('stressTest: ran %d iterations\n', r[1]))
}

test99 <- function() {
  cat('shutting down SMP workers...\n')
  stopWorkers(w)
  cat('shutdown complete\n')
}
