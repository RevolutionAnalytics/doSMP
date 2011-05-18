registerDoSMP <- function(w) {
  if (!inherits(w, 'workergroup'))
    stop('w must be a worker group object returned by startWorkers')

  setDoPar(doSMP, w, info)
}

info <- function(data, item) {
  switch(item,
         workers=data$workerCount,
         name='doSMP',
         version=packageDescription('doSMP', fields='Version'),
         NULL)
}

makeDotsEnv <- function(...) {
  list(...)
  function() NULL
}

comp <- if (getRversion() < "2.13.0") {
  function(expr, ...) expr
} else {
  compiler::compile
}

doSMP <- function(obj, expr, envir, data) {
  info <- obj$verbose
  w <- data

  if (!inherits(obj, 'foreach'))
    stop('obj must be a foreach object')

  it <- iter(obj)

  # check for smp-specific options
  options <- obj$options$smp
  if (!is.null(options)) {
    nms <- names(options)
    recog <- nms %in% c('info', 'chunkSize', 'initEnvir', 'initArgs',
                        'finalEnvir', 'finalArgs')
    if (any(!recog))
      warning(sprintf('ignoring unrecognized smp option(s): %s',
                      paste(nms[!recog], collapse=', ')), call.=FALSE)

    if (!is.null(options$chunkSize)) {
      if (!is.numeric(options$chunkSize) || length(options$chunkSize) != 1) {
        warning('chunkSize must be a numeric value', call.=FALSE)
        options$chunkSize <- NULL  # remove from options
      }
    }

    if (!is.null(options$info)) {
      if (!is.logical(options$info) || length(options$info) != 1) {
        warning('info must be a logical value', call.=FALSE)
        options$info <- NULL  # remove from options
      } else {
        info <- options$info
      }
    }

    if (!is.null(options$initEnvir)) {
      if (!is.function(options$initEnvir)) {
        warning('initEnvir must be a function', call.=FALSE)
        options$initEnvir <- NULL  # remove from options
      }
    }

    if (!is.null(options$initArgs)) {
      if (!is.list(options$initArgs)) {
        warning('initArgs must be a list', call.=FALSE)
        options$initArgs <- NULL  # remove from options
      }
    }

    if (!is.null(options$finalEnvir)) {
      if (!is.function(options$finalEnvir)) {
        warning('finalEnvir must be a function', call.=FALSE)
        options$finalEnvir <- NULL  # remove from options
      }
    }

    if (!is.null(options$finalArgs)) {
      if (!is.list(options$finalArgs)) {
        warning('finalArgs must be a list', call.=FALSE)
        options$finalArgs <- NULL  # remove from options
      }
    }
  }

  # setup the parent environment by first attempting to create an environment
  # that has '...' defined in it with the appropriate values
  exportenv <- tryCatch({
    qargs <- quote(list(...))
    args <- eval(qargs, envir)
    environment(do.call(makeDotsEnv, args))
  },
  error=function(e) {
    new.env(parent=emptyenv())
  })
  noexport <- union(obj$noexport, obj$argnames)
  getexports(expr, exportenv, envir, bad=noexport)
  vars <- ls(exportenv)
  if (info) {
    if (length(vars) > 0) {
      cat('automatically exporting the following variables',
          'from the local environment:\n')
      cat(' ', paste(vars, collapse=', '), '\n')
    } else {
      cat('no variables are automatically exported\n')
    }
  }

  # compute list of variables to export
  export <- unique(obj$export)
  ignore <- intersect(export, vars)
  if (length(ignore) > 0) {
    warning(sprintf('already exporting variable(s): %s',
            paste(ignore, collapse=', ')), call.=FALSE)
    export <- setdiff(export, ignore)
  }

  # add explicitly exported variables to exportenv
  if (length(export) > 0) {
    if (info)
      cat(sprintf('explicitly exporting variables(s): %s\n',
                  paste(export, collapse=', ')))

    for (sym in export) {
      if (!exists(sym, envir, inherits=TRUE))
        stop(sprintf('unable to find variable "%s"', sym))
      assign(sym, get(sym, envir, inherits=TRUE),
             pos=exportenv, inherits=FALSE)
    }
  }

  if (info) {
    varNames <- ls(exportenv)
    if (length(varNames) > 0) {
      total <- 0
      cat('\nexported var size (approximate/bytes)\n')
      cat('--------------------------------------\n')
      for(v in varNames) {
        objsize <- object.size(get(v, pos=exportenv, inherits=FALSE))
        cat(sprintf('%-26s %10d\n', v, objsize))
        total <- total + objsize
      }
      cat('======================================\n')
      cat(sprintf('%-26s %10d\n\n', 'total', total))
    }
  }

  # compile the expression if we're using R 2.13.0 or greater
  xpr <- comp(expr, env=envir, options=list(suppressUndefined=TRUE))

  options$verbose <- obj$verbose

  # execute the tasks
  engine(w, it, xpr, exportenv, obj$packages, options)

  # check for errors
  errorValue <- getErrorValue(it)
  errorIndex <- getErrorIndex(it)

  # throw an error or return the combined results
  if (identical(obj$errorHandling, 'stop') && !is.null(errorValue)) {
    msg <- sprintf('task %d failed - "%s"', errorIndex,
                   conditionMessage(errorValue))
    stop(simpleError(msg, call=expr))
  } else {
    getResult(it)
  }
}
