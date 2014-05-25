\name{list_databases}
\alias{list_databases}
\alias{list_databases.sqliter}
\title{lists databases into path}
\usage{
list_databases(object, filter = "")

\method{list_databases}{sqliter}(object, filter = "")
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
list_databases(DBM)
list_databases(DBM, 'fu')
}

