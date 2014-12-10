context('create sqliter')

test_that('it should create sqliter', {
  DBM <- sqliter(path='.')
  expect_is(DBM, 'sqliter')
  expect_equal(DBM$get('path'), '.')
  DBM <- sqliter()
  expect_equal(DBM$get('path'), '.')
})

context('database functions')

test_that('it should list tables in each database', {
  DBM <- sqliter(path='.')
  res <- query(DBM, 'funds', "select tbl_name from sqlite_master where type = 'table'")
})

test_that('it should list databases', {
  DBM <- sqliter(path='.')
  dbs <- databases(DBM)
  expect_is(dbs, 'dbl')
  expect_true(length(databases(DBM)) == 3)
})

test_that('it should list databases using filter', {
  DBM <- sqliter(path='.')
  dbs <- databases(DBM, 'fu')
  expect_true(length(dbs) == 1)
  expect_true(dbs[[1]]$database == 'funds')
})

test_that("it should list tables from a database", {
  DBM <- sqliter(path=c('.', 'db'))
  tbls <- tables(DBM, 'funds')
  expect_is(tbls, 'entity_lists')
  # expect_true(length(tbls) == 1)
  # expect_true(tbls[[1]]$database == 'funds')
  # expect_true(length(tbls[[1]]$entity_names) == 3)
  tbls <- tables(DBM, 'dup')
  # expect_true(length(tbls) == 2)
  # expect_true(length(tbls[[1]]$entity_names) == 2)
  # expect_true(length(tbls[[2]]$entity_names) == 3)
})

context('execute query')

test_that('it should execute a queries from file', {
  DBM <- sqliter(path='.')
  DB <- databases(DBM)[['test']]
  res <- query_from_file(DB, 'database1.sql')
  expect_equal(length(res), 9)
  expect_is(res[[9]], 'data.frame')
})

test_that('it should execute a query', {
  DBM <- sqliter(path='.')
  res <- query(DBM, 'funds', 'select count(*) from sqlite_master')
  expect_equal(as.numeric(unlist(res)), 3)
})

test_that('it should create db file', {
  fname <- 'test2.db'
  on.exit(unlink(fname))

  expect_false(file.exists(fname))
  DB <- as.db(fname)
  res <- query_from_file(DB, 'database1.sql')
  expect_true(file.exists(fname))
  expect_equal(length(res), 9)
  expect_is(res[[9]], 'data.frame')
})

test_that('it should send query', {
  fname <- 'main.db'
  on.exit(unlink(fname))
  
  DB <- as.db(fname)
  query_from_file(DB, 'database1.sql')
  res <- send_query(DB, 'insert into test1 values (5, "e")')
  expect_true(res)
  res <- send_query(DB, 'insert into test1 values (?, ?)', c1=11:20, c2=letters[11:20])
  expect_true(res)
  df <- data.frame(c1=1:20, c2=letters[1:20], stringsAsFactors=FALSE)
  res <- send_query(DB, 'insert into test2 values (?, ?)', df)
  expect_true(res)
  expect_equal(query(DB, 'select * from test2'), df)
})

test_that('it should send query again', {
  fname <- 'main.db'
  on.exit(unlink(fname))
  
  DB <- as.db(fname)
  query_from_file(DB, 'database1.sql')
  res <- send_query(DB, 'insert into test2 values (?, ?)', c1=11:20, c2=letters[11:20], func=function(r){
    DBI::dbGetRowsAffected(r)
  })
  df <- data.frame(c1=11:20, c2=letters[11:20], stringsAsFactors=FALSE)
  expect_equal(query(DB, 'select * from test2'), df)
  expect_equal(res, 1)
  res <- send_query(DB, 'update test2 set c2 = ? where c1 = ?', letters[1:10], 11:20, func=function(r){
    DBI::dbGetRowsAffected(r)
  })
  df <- data.frame(c1=11:20, c2=letters[1:10], stringsAsFactors=FALSE)
  expect_equal(query(DB, 'select * from test2'), df)
  expect_equal(res, 1)
})


test_that('it should send query again then again', {
  fname <- 'main.db'
  on.exit(unlink(fname))
  
  DB <- as.db(fname)
  query_from_file(DB, 'database1.sql')
  res <- send_query(DB, 'insert into test2 (c2) values (?)', c2=letters[11:20])
  df <- data.frame(c1=as.character(NA), c2=letters[11:20], stringsAsFactors=FALSE)
  expect_equal(query(DB, 'select * from test2'), df)
})


test_that('it should inspect empty databases', {
  fname <- 'main.db'
  on.exit(unlink(fname))
  
  DB <- as.db(fname)
  expect_true(is.null(tables(DB)))
})


test_that('it should access tables', {
  fname <- 'main.db'
  on.exit(unlink(fname))
  
  DB <- as.db(fname)
  query_from_file(DB, 'database1.sql')
  print(tables(DB)['test1'])
})

context('error message')

test_that('it should check error message', {
  DBM <- sqliter(path='.')
  expect_error(query(DBM, 'xxx', 'select count(*) from sqlite_master'),
    "Database not found: xxx")
})

context('duplicates')

test_that('it should handle duplicates', {
  DBM <- sqliter(path=c('.', 'db'))
  dbs <- databases(DBM, 'dup')
  expect_true(length(dbs) == 2)
  expect_equal(unname(sapply(dbs, function(x) x$database)), c('dup', 'dup'))
  res <- query(DBM, 'dup', 'select count(*) from sqlite_master')
  expect_equal(as.numeric(unlist(res)), 2)
  res <- query(DBM, 'dup', 'select count(*) from sqlite_master', index=2)
  expect_equal(as.numeric(unlist(res)), 3)
})

context('helpers')

test_that('it should test rac function', {
  DBM <- sqliter()
  got <- query(DBM, 'test', 'select * from test1', post_proc=rac(,1))
  expect_equal(1:4, got)
  got <- query(DBM, 'test', 'select * from test1', post_proc=rac(1))
  expect_equal(1, got$c1)
  expect_equal('a', got$c2)
  got <- query(DBM, 'test', 'select * from test1', post_proc=rac(1,2))
  expect_equal('a', got)
})
