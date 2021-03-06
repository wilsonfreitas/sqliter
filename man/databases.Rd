% Generated by roxygen2 (4.0.2): do not edit by hand
\name{databases}
\alias{databases}
\alias{databases.sqliter}
\title{lists databases into path}
\usage{
databases(object, filter = "")

\method{databases}{sqliter}(object, filter = "")
}
\arguments{
\item{object}{\code{sqliter} object}

\item{filter}{}
}
\description{
lists databases into path
}
\examples{
DBM <- sqliter(path='data')
databases(DBM)
databases(DBM, 'fu')
}

