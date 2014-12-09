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
  files <- do.call(rbind, lapply(object$get('path'), function(x) {
    path <- list.files(x, '*.db', full.names=TRUE)
    path
  }))
  databases <- apply(files, 2, as.db)
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
  as.entity_list(entities, object$database)
}

#' @export
tables <- function(object, database='') UseMethod('tables', object)

#' @export
tables.sqliter <- function(object, database='') {
  en <- entities(object, database)
  as.entity_lists(subset(en, type == 'table'))
}

#' @export
runDB <- function(db, func) {
  conn <- dbConnect(RSQLite::SQLite(), db$path)
  x <- func(conn)
  dbDisconnect(conn)
  x
}

#' @export
tables.db <- function(object, database='') {
  en <- entities(object)
  as.entity_list(subset(en, type == 'table'), object$database)
}

#' @export
indexes <- function(object, database='') UseMethod('indexes', object)

#' @export
indexes.sqliter <- function(object, database='') {
  en <- entities(object, database)
  as.entity_lists(subset(en, type == 'index'))
}

#' @export
indexes.db <- function(object) {
  en <- entities(object)
  as.entity_list(subset(en, type == 'index'), object$database)
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

query.db <- function(object, query, post_proc=identity, ...) {
  database <- object
  conn <- dbConnect(RSQLite::SQLite(), database$path)
  if (length(list(...)) != 0) {
    ds <- dbGetPreparedQuery(conn, query, data.frame(...))
  } else {
    ds <- dbGetQuery(conn, query)
  }
  dbDisconnect(conn)
  post_proc(ds)
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
  parts <- str_split(str_replace(path, '\\.db', ''), '/')
  database <- parts[[1]][length(parts[[1]])]
  structure(list(database=database, path=path), class='db')
}

#' @export
print.db <- function(x, ...) {
  asciify(data.frame(database=x$database, path=x$path))
  # cat(x$database, x$path, '\n')
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

#' @export
as.entity_list <- function(entity_list, database) {
  if (dim(entity_list)[1] == 0)
    return(NULL)
  entity_list$database <- database
  attr(entity_list, 'database') <- database
  structure(entity_list, class=c('entity_list', 'data.frame'))
}

#' @export
print.entity_list <- function(x, ...) {
  cat("Database:", attr(x, 'database'), '\n')
  asciify(x[,c('name', 'type', 'sql')])
  x
}

#' @export
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
    ## internal functions
    SepLine <- function(n, pad = 1) {
        tmp <- lapply(n, function(x, pad) paste(rep("-", x + (2* pad)),
                                                collapse = ""),
                      pad = pad)
        paste0("+", paste(tmp, collapse = "+"), "+")
    }
    Row <- function(x, n, pad = 1) {
        foo <- function(i, x, n) {
            fmt <- paste0("%", n[i], "s")
            sprintf(fmt, as.character(x[i]))
        }
        rowc <- sapply(seq_along(x), foo, x = x, n = n)
        paste0("|", paste(paste0(rep(" ", pad), rowc, rep(" ", pad)),
                          collapse = "|"),
               "|")
    }
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
        writeLines(sep)
    }
    invisible(df)
}