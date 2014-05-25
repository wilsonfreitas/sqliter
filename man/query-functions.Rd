\name{query-functions}
\alias{query-functions}
\title{query functions}
\description{
**query functions** are dynamic functions which connect to
a database, execute queries in it and transform data.
Actually it is a decorator for \code{execute} function.
\code{execute} has 5 arguments. The first argument is an
instance of the \code{sqliter} class and the second is the
database name. The call to a query function is executed
like a method call to the \code{sqliter} object through the
\code{$} operator. The function name must have the
following pattern: \code{query_<database name without
extension>}. This call returns an \code{execute} function
with the first 2 argument already set. The first parameter
is the \code{sqliter} object on which the \code{$} operator
have been called and the second argument is extracted from
the query function name, the name after the preffix
\code{query_}.
}
\examples{
\dontrun{
DBM <- sqliter(path=c("data", "another/project/data"))
DBM$query_dummydatabase("select count(*) from dummytable")
}
}

