msc.quote <- function(arg) {
  # argument needs quoting if it contains whitespace or a double-quote
  if (length(arg) != 1)
    stop('length of arg must be 1')

  if (!nzchar(arg)) {
    # empty strings must be quoted
    '""'
  } else if (length(grep('[[:space:]"]', arg)) == 0) {
    arg
  }
  else {
    q <- '"'
    nbs <- 0
    v <- strsplit(arg, split='')[[1]]
    for (c in v) {
      if (c == '\\') {
        q <- paste(q, c, sep='')
        nbs <- nbs + 1
      }
      else if (c == '"') {
        q <- paste(q, paste(rep('\\', nbs + 1), collapse=''), c, sep='')
        nbs <- 0
      }
      else {
        q <- paste(q, c, sep='')
        nbs <- 0
      }
    }

    paste(q, paste(rep('\\', nbs), collapse=''), '"', sep='')
  }
}

simple.quote <- function(arg) {
  if (length(arg) != 1)
    stop('length of arg must be 1')

  if (!nzchar(arg)) {
    # empty strings must be quoted
    '""'
  } else if (length(grep('"', arg)) > 0) {
    stop('arguments cannot contain double quotes with simple quoting')
  }
  else if (length(grep('[[:space:]]', arg)) == 0) {
    # argument without whitespace and double-quotes don't need quoting
    arg
  }
  else {
    paste('"', arg, '"', sep='')
  }
}

posix.quote <- function(arg) {
  if (length(arg) != 1)
    stop('length of arg must be 1')

  q <- "'"
  v <- strsplit(arg, split='')[[1]]
  for (c in v) {
    if (c == "'") {
      c <- "'\\''"
    }
    q <- paste(q, c, sep='')
  }

  paste(q, "'", sep='')
}

argv2str <- function(argv, quoting) {
  qfun <- if (is.null(quoting)) {
    if (.Platform$OS.type == 'windows')
      msc.quote
    else
      posix.quote
  } else if (quoting == 'posix')
    posix.quote
  else if (quoting == 'msc')
    msc.quote
  else if (quoting == 'simple')
    simple.quote
  else
    stop('unrecognized quoting method: ', quoting)

  paste(lapply(argv, qfun), collapse=' ')
}
