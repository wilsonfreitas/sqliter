

sqliter <- function(...) {
    defaults <- list(...)
    
    get <- function(name, default=FALSE, drop=TRUE) {
        if (default)
            defaults <- value  # this is only a local version
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
    merge <- function(values) merge_list(defaults, values)
    restore <- function(target = value) defaults <<- target
    
    this <- list(get=get, set=set, merge=merge, restore=restore)
    class(this) <- 'sqliter'
    this
}

find_database <- function(object, ...) UseMethod('find_database', object)

find_database.sqliter <- function(handler, database) {
    path <- paste0(handler$get('path'), database, '.db')
    res <- sapply(path, file.exists)
    path[res]
}

execute <- function(object, ...) UseMethod('execute', object)

execute.sqliter <- function(handler, database, query, post_proc=identity, ...) {
    path <- find_database(handler, database)
    stopifnot(length(path) == 1)
    conn <- dbConnect('SQLite', path)
    if (length(list(...)) != 0) {
        ds <- dbGetPreparedQuery(conn, query, data.frame(...))
    } else {
        ds <- dbGetQuery(conn, query)
    }
    dbDisconnect(conn)
    post_proc(ds)
}

'$.sqliter' <- function(handler, name) {
    if (str_detect(name, "^query_(.*)$")) {
        database <- unlist(str_split_fixed(name, "_", 2))[2]
        Curry(execute, handler, database)
    } else {
        handler[[name]]
    }
}


