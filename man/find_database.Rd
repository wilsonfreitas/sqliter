\name{find_database}
\alias{find_database}
\alias{find_database.sqliter}
\title{returns the paths of the given database}
\usage{
find_database(object, database)

\method{find_database}{sqliter}(object, database)
}
\arguments{
  \item{object}{\code{sqliter} object}

  \item{database}{the SQLite database filename without
  extension}
}
\description{
returns the paths of the given database
}
\examples{
\dontrun{
DBM <- sqliter(path=c("data", "another/project/data"))
find_database(DBM, "dummydatabase")
# "data/dummydatabase.db"
}
}

