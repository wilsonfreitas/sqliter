\name{execute}
\alias{execute}
\alias{execute.sqliter}
\title{execute query into a given database}
\usage{
execute(object, ...)

\method{execute}{sqliter}(object, database, query, post_proc = identity,
  index = 1, ...)
}
\arguments{
  \item{object}{\code{sqliter} object}

  \item{database}{the SQLite database filename without
  extension}

  \item{query}{the query string}

  \item{post_proc}{a function to transform data, it
  receives a database and returns whatever you need.}

  \item{...}{additional arguments used by prepared queries}
}
\description{
Once you have a \code{sqliter} database properly set you
can execute queries into that database and get your data
transformed. By default this function returns a data.frame
object, but if you transform your data you can get whatever
you need.
}
\examples{
\dontrun{
DBM <- sqliter(path=c("data", "another/project/data"))
ds <- execute(DBM, "dummydatabase", "select count(*) from dummytable")
ds <- execute(DBM, "dummydatabase", "select * from dummytable where
      name = :name", name=c("Macunamima", "Borba Gato"))
ds <- execute(DBM, "dummydatabase", "select * from dummytable where
      name = :name", name=c("Macunamima", "Borba Gato"),
        post_proc=function(ds) {
ds <- transform(ds, birthday=as.Date(birthday))
ds
})
}
}

