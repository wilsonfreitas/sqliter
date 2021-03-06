
library(DBI)
library(RSQLite)
library(stringr)
library(functional)


#' Functions to wrap SQLite calls
#' 
#' sqliter helps users, mainly data munging practioneers, to organize
#' their sql calls in a clean structure. It simplifies the process of
#' extracting and transforming data into useful formats.
#'
#' @name sqliter-package
#' @docType package
#' @import stringr
#' @import RSQLite
#' @import DBI
#' @import functional
NULL

#' Creates the sqliter a kinf of SQLite database manager, but not that far.
#' 
#' \code{sqliter} object works pretty much like a database manager helping users to execute queries and transform data through a clean interface.
#' 
#' @export
#' @param ... arguments such as \code{path} must be provided during object instantiation.
#' @examples
#' \dontrun{DBM <- sqliter(path=c("data", "another/project/data"))}
#' 
sqliter <- function(path='.', ...) {
  defaults <- list(path=sub('\\/$', '', path), ...)
  
  get <- function(name, drop=TRUE) {
    if (missing(name))
      defaults
    else {
      if (drop && length(name) == 1)
        defaults[[name]]
      else
        defaults[name]
    }
  }
	
  set <- function(...) {
    dots <- list(...)
    if (length(dots) == 0) return()
      if (is.null(names(dots)) && length(dots) == 1 && is.list(dots[[1]]))
        if (length(dots <- dots[[1]]) == 0) return()
          defaults <<- merge(dots)
    invisible(NULL)
  }
    
  this <- list(get=get, set=set)
  class(this) <- 'sqliter'
  this
}

#' lists databases into path
#' 
#' @param object \code{sqliter} object
#' @param filter
#' @export
#' @examples
#' DBM <- sqliter(path='data')
#' databases(DBM)
#' databases(DBM, 'fu')
databases <- function(object, filter='') UseMethod('databases', object)

#' @rdname databases
#' @export
databases.sqliter <- function(object, filter='') {
  files <- do.call(c, lapply(object$get('path'), function(x) {
    path <- list.files(x, '*.db', full.names=TRUE)
    path
  }))
  databases <- lapply(files, as.db)
  dbl <- Filter(function(x) str_detect(x$database, filter), databases)
  db_names <- sapply(dbl, function(db) db$database)
  names(dbl) <- db_names
  as.dbl(dbl)
}

entities <- function(object, database='')  UseMethod('entities', object)

entities.sqliter <- function(object, database='') {
  dbs <- databases(object, database)
  entity_lists <- lapply(dbs, entities)
  as.entity_lists(do.call(rbind, entity_lists))
}

entities.db <- function(object) {
  query <- "select name, sql, type from sqlite_master"
  entities <- query(object, query)
  as.entity_list(entities, object)
}

#' @export
tables <- function(object, ...) UseMethod('tables', object)

#' @export
tables.sqliter <- function(object, database='') {
  en <- entities(object, database)
  if (is.null(en))
    NULL
  else
    as.entity_lists(subset(en, type == 'table'))
}

#' @export
tables.db <- function(object, ...) {
  en <- entities(object)
  if (is.null(en))
    NULL
  else
    as.entity_list(subset(en$data, type == 'table'), object)
}

#' @export
indexes <- function(object, ...) UseMethod('indexes', object)

#' @export
indexes.sqliter <- function(object, database='', ...) {
  en <- entities(object, database)
  if (is.null(EN))
    NULL
  else
    as.entity_lists(subset(en, type == 'index'))
}

#' @export
indexes.db <- function(object, ...) {
  en <- entities(object)
  if (is.null(EN))
    NULL
  else
    as.entity_list(subset(en, type == 'index'), object)
}

#' execute query into a given database
#' 
#' Once you have a \code{sqliter} database properly set you can execute queries into that database and get your data transformed.
#' By default this function returns a data.frame object, but if you transform your data you can get whatever you need.
#' 
#' @param object \code{sqliter} object
#' @param database the SQLite database filename without extension
#' @param query the query string
#' @param post_proc a function to transform data, it receives a database and returns whatever you need.
#' @param ... additional arguments used by prepared queries
#' @export
#' @examples
#' \dontrun{
#' DBM <- sqliter(path=c("data", "another/project/data"))
#' ds <- query(DBM, "dummydatabase", "select count(*) from dummytable")
#' ds <- query(DBM, "dummydatabase", "select * from dummytable where 
#'       name = :name", name=c("Macunamima", "Borba Gato"))
#' ds <- query(DBM, "dummydatabase", "select * from dummytable where
#'       name = :name", name=c("Macunamima", "Borba Gato"),
#'         post_proc=function(ds) {
#' ds <- transform(ds, birthday=as.Date(birthday))
#' ds
#' })
#' }
query <- function(object, ...) UseMethod('query', object)


#' @rdname query
#' @export
query.sqliter <- function(object, database, query, post_proc=identity, index=1, ...) {
  path <- databases(object, database)
  if (length(path) == 0)
    stop("Database not found: ", database)
  database <- path[[index]]
  query(database, query, post_proc, ...)
}


#' @export
query.db <- function(object, query, post_proc=identity, ...) {
  ds <- execute_in_db(object, function(conn, ...) {
    if (length(list(...)) != 0)
      RSQLite::dbGetPreparedQuery(conn, query, data.frame(...))
    else
      DBI::dbGetQuery(conn, query)
  }, ...)
  post_proc(ds)
}


sql_from_file <- function(file){
  sql <- readLines(file)
  sql <- unlist(str_split(paste(sql,collapse=" "),";"))
  sql <- sql[grep("^ *$", sql, invert=T)]
  sql
}


query_many <- function(con, sql) {
  lapply(sql, function(.sql) {
    res <- DBI::dbSendQuery(con, .sql)
    on.exit(DBI::dbClearResult(res))
    info <- DBI::dbGetInfo(res)
    if (info$isSelect == 1)
      DBI::dbFetch(res)
    else
      c(DBI::dbHasCompleted(res) == 1, DBI::dbGetRowsAffected(res))
  })
}

#' @export
query_from_file <- function(db, file) UseMethod('query_from_file', db)

#' @export
query_from_file.db <- function(db, file) {
  sql <- sql_from_file(file)
  x <- execute_in_db(db, query_many, sql)
  invisible(x)
}

#' @export
send_query <- function(db, query, ..., func) UseMethod('send_query', db)

#' @export
send_query.db <- function(db, query, ..., func=function(r) DBI::dbHasCompleted(r) == 1) {
  execute_in_db(db, function(conn) {
    if (length(list(...)) == 0)
      res <- DBI::dbSendQuery(conn, query)
    else
      res <- RSQLite::dbSendPreparedQuery(conn, query, data.frame(...))
    on.exit(DBI::dbClearResult(res))
    func(res)
  })
}

#' @export
execute_in_db <- function(db, func, ...) {
  conn <- DBI::dbConnect(RSQLite::SQLite(), db$path)
  on.exit(DBI::dbDisconnect(conn))
  func(conn, ...)
}

#' @export
'$.sqliter' <- function(object, name) {
  if (str_detect(name, "^query_(.*)$")) {
    database <- unlist(str_split_fixed(name, "_", 2))[2]
    Curry(query, object, database)
  } else {
    object[[name]]
  }
}

#' query functions
#' 
#' **query functions** are dynamic functions which connect to a database, execute queries in it and transform data.
#' Actually it is a decorator for \code{query} function.
#' \code{query} has 5 arguments.
#' The first argument is an instance of the \code{sqliter} class and the second is the database name.
#' The call to a query function is executed like a method call to the \code{sqliter} object through the \code{$} operator.
#' The function name must have the following pattern: \code{query_<database name without extension>}.
#' This call returns an \code{query} function with the first 2 argument already set.
#' The first parameter is the \code{sqliter} object on which the \code{$} operator have been called and the second argument is extracted from the query function name, the name after the preffix \code{query_}.
#' 
#' @name query-functions
#' @examples
#' \dontrun{
#' DBM <- sqliter(path=c("data", "another/project/data"))
#' DBM$query_dummydatabase("select count(*) from dummytable")
#' }
NULL

#' @export
as.db <- function(path) {
  if (str_detect(path, 'db$')) {
    parts <- str_split(str_replace(path, '\\.db', ''), '/')
    database <- parts[[1]][length(parts[[1]])]
  } else stop(paste("Invalid db path:", path))
  structure(list(database=database, path=path), class='db')
}

#' @export
print.db <- function(x, ...) {
  asciify(data.frame(database=x$database, path=x$path))
  x
}

#' @export
as.dbl <- function(dbl) {
  structure(dbl, class='dbl')
}

#' @export
print.dbl <- function(x, ...) {
  asciify(as.data.frame(do.call(rbind, x)))
  x
}

as.entity_list <- function(entity_list, database) {
  that <- list()
  if (is.null(entity_list) || dim(entity_list)[1] == 0)
    return(NULL)
  entity_list$database <- attr(database, 'database')
  that$data <- entity_list
  that$database <- database
  structure(that, class='entity_list')
}

#' @export
print.entity_list <- function(x, ...) {
  cat("Database:", x$database$database, '\n')
  asciify(x$data[,c('name', 'type', 'sql')])
  x
}

#' @export
'[[.entity_list' <- function(x, r, ...) {
  if (any(x$data$name == r)) {
    query(x$database, paste('select * from', r))
  } else stop(paste('Invalid table name:', r))
}

as.entity_lists <- function(entity_lists) structure(entity_lists, class=c('entity_lists', 'data.frame'))

#' @export
print.entity_lists <- function(x, ...) {
  asciify(x[,c('database', 'name', 'type', 'sql')])
  x
}

#' @export
print.sqliter <- function(x, ...) {
  path <- x$get('path')
  cat("sqliter class", "\n\n")
  cat("Path:", "\n")
  for (dir in path)
    cat(dir, '\n')
  cat("\n")
  x
}

#' @export
rac <- function(i, j) {
  if (missing(i))
    function(df) df[,j]
  else if (missing(j))
    function(df) df[i,]
  else
    function(df) df[i,j]
}

asciify <- function(df, pad = 1, ...) {
  ## error checking
  stopifnot(is.data.frame(df))
  df <- asciify_prepare(df)
  ## internal functions
  SepLine <- function(n, pad = 1) {
    tmp <- lapply(n, function(x, pad) {
      paste(rep("-", x + (2* pad)), collapse = "")
    }, pad = pad)
    paste0("+", paste(tmp, collapse = "+"), "+")
  }
  Row <- function(x, n, pad = 1) {
    foo <- function(i, x, n) {
      fmt <- paste0("%", n[i], "s")
      sprintf(fmt, as.character(x[i]))
    }
    rowc <- sapply(seq_along(x), foo, x = x, n = n)
    paste0("|", paste(paste0(rep(" ", pad), rowc, rep(" ", pad)), collapse = "|"), "|")
  }
  .rowid <- df$.asciify.row
  df$.asciify.row <- NULL
  ## convert everything to characters
  df <- as.matrix(df)
  ## nchar in data
  mdf <- apply(df, 2, function(x) max(nchar(x)))
  ## nchar in names
  cnames <- nchar(colnames(df))
  ## max nchar of name+data per elements
  M <- pmax(mdf, cnames)
  ## write the header
  sep <- SepLine(M, pad = pad)
  writeLines(sep)
  writeLines(Row(colnames(df), M, pad = pad))
  writeLines(sep)
  ## write the rows
  for(i in seq_len(nrow(df))) {
    ## write a row
    writeLines(Row(df[i,], M, pad = pad))
    ## write separator
    if (i == length(.rowid) || .rowid[i] != .rowid[i+1])
      writeLines(sep)
  }
  invisible(df)
}


asciify_prepare <- function(df) {
  df <- lapply(split(df, rownames(df)), function(x) {
    x <- unclass(x)
    ds <- lapply(x, function(s) {
      str_split(as.character(s), '\n')[[1]]
    })
    m <- max(sapply(ds, length))
    ds <- lapply(ds, function(s) {
      length(s) <- m
      s[is.na(s)] <- ""
      s
    })
    ds <- do.call(cbind, ds)
    ds <- as.data.frame(ds)
    ds$.asciify.row <- attr(x, "row.names")
    ds
  })
  do.call(rbind, df)
}

# df <- data.frame(int=c(11, 12), name=c('wilson\nfreitas', 'wilson\nfreitas'), stringsAsFactors=FALSE)
# asciify(asciify_prepare(df))
#
# df <- data.frame(int=c(11, 12), name=c('wilson', 'freitas'), stringsAsFactors=FALSE)
# asciify(asciify_prepare(df))
