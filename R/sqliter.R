#' Functions to wrap SQLite calls
#' 
#' sqliter helps users, mainly data munging practioneers, to organize
#' their sql calls in a clean structure. It simplifies the process of
#' extracting and transforming data into useful formats.
#'
#' @name sqliter-package
#' @docType package
#' @import stringr
#' @import DBI
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
  defaults <- list(path=path, ...)
    
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
#' list_databases(DBM)
#' list_databases(DBM, 'fu')
list_databases <- function(object, filter='') UseMethod('list_databases', object)

#' @rdname list_databases
#' @export
list_databases.sqliter <- function(object, filter='') {
  databases <- do.call(rbind, lapply(object$get('path'), function(x) {
    database <- str_replace(list.files(x, '*.db'), '\\.db', '')
    path <- list.files(x, '*.db', full.names=TRUE)
    cbind(database, path)
  }))
  databases <- apply(databases, 1, function(x) {
    as.db(x[1], x[2])
  })
  as.dbl(Filter(function(x) str_detect(x$database, filter), databases))
}

list_entities <- function(object, filter, type, label) {
  query <- "select name from sqlite_master where type = :type"
  dbs <- list_databases(object, filter)
  entity_list <- lapply(dbs, function(x) {
    entity_names <- execute(x, query, type=type, post_proc=function(ds) {
      ds$name
    })
    list(database=x$database, entity_names=entity_names)
  })
  as.entity_lists(lapply(entity_list, function(x) as.entity_list(x, label)))
}

#' @export
list_tables <- function(object, filter='') UseMethod('list_tables', object)

#' @export
list_tables.sqliter <- function(object, filter='')
  list_entities(object, filter, 'table', 'Tables')

#' @export
list_indexes <- function(object, filter='') UseMethod('list_indexes', object)

#' @export
list_indexes.sqliter <- function(object, filter='')
  list_entities(object, filter, 'index', 'Indexes')

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
#' ds <- execute(DBM, "dummydatabase", "select count(*) from dummytable")
#' ds <- execute(DBM, "dummydatabase", "select * from dummytable where 
#'       name = :name", name=c("Macunamima", "Borba Gato"))
#' ds <- execute(DBM, "dummydatabase", "select * from dummytable where
#'       name = :name", name=c("Macunamima", "Borba Gato"),
#'         post_proc=function(ds) {
#' ds <- transform(ds, birthday=as.Date(birthday))
#' ds
#' })
#' }
execute <- function(object, ...) UseMethod('execute', object)

#' @rdname execute
#' @export
execute.sqliter <- function(object, database, query, post_proc=identity,
index=1, ...) {
  path <- list_databases(object, database)
  if (length(path) == 0)
    stop("Database not found: ", database)
  database <- path[[index]]
  execute(database, query, post_proc, ...)
}

execute.db <- function(object, query, post_proc=identity, ...) {
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
    Curry(execute, object, database)
  } else {
    object[[name]]
  }
}

#' query functions
#' 
#' **query functions** are dynamic functions which connect to a database, execute queries in it and transform data.
#' Actually it is a decorator for \code{execute} function.
#' \code{execute} has 5 arguments.
#' The first argument is an instance of the \code{sqliter} class and the second is the database name.
#' The call to a query function is executed like a method call to the \code{sqliter} object through the \code{$} operator.
#' The function name must have the following pattern: \code{query_<database name without extension>}.
#' This call returns an \code{execute} function with the first 2 argument already set.
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
as.db <- function(database, path) {
  structure(list(database=database, path=path), class='db')
}

#' @export
print.db <- function(x, ...) {
  cat(x$database, x$path, '\n')
  x
}

#' @export
as.dbl <- function(dbl) {
  structure(dbl, class='dbl')
}

#' @export
print.dbl <- function(x, ...) {
  for (db in x)
    print(db)
  x
}

#' @export
as.entity_list <- function(database, label='') {
  structure(database, class='entity_list', label=label)
}

#' @export
print.entity_list <- function(x, ...) {
  cat("Database:", x$database, '\n')
  cat(paste0(attr(x, 'label'), ":"), x$entity_names, '\n\n')
  x
}

#' @export
as.entity_lists <- function(entity_lists) {
  structure(entity_lists, class='entity_lists')
}

#' @export
print.entity_lists <- function(x, ...) {
  for (entity_list in x)
    print(entity_list)
  x
}