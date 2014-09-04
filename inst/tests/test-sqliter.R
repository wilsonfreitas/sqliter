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
  res <- execute(DBM, 'funds', "select tbl_name from sqlite_master where type = 'table'")
})

test_that('it should list databases', {
  DBM <- sqliter(path='.')
  dbs <- list_databases(DBM)
  expect_is(dbs, 'dbl')
  expect_true(length(list_databases(DBM)) == 3)
})

test_that('it should list databases using filter', {
  DBM <- sqliter(path='.')
  dbs <- list_databases(DBM, 'fu')
  expect_true(length(dbs) == 1)
  expect_true(dbs[[1]]$database == 'funds')
})

test_that("it should list tables from a database", {
  DBM <- sqliter(path=c('.', 'db'))
  tbls <- list_tables(DBM, 'funds')
  expect_is(tbls, 'entity_lists')
  expect_true(length(tbls) == 1)
  expect_true(tbls[[1]]$database == 'funds')
  expect_true(length(tbls[[1]]$entity_names) == 3)
  tbls <- list_tables(DBM, 'dup')
  expect_true(length(tbls) == 2)
  expect_true(length(tbls[[1]]$entity_names) == 2)
  expect_true(length(tbls[[2]]$entity_names) == 3)
})

context('execute query')

test_that('it should execute a query', {
  DBM <- sqliter(path='.')
  res <- execute(DBM, 'funds', 'select count(*) from sqlite_master')
  expect_equal(as.numeric(unlist(res)), 3)
})

context('error message')

test_that('it should check error message', {
  DBM <- sqliter(path='.')
  expect_error(execute(DBM, 'xxx', 'select count(*) from sqlite_master'),
    "Database not found: xxx")
})

context('duplicates')

test_that('it should handle duplicates', {
  DBM <- sqliter(path=c('.', 'db'))
  dbs <- list_databases(DBM, 'dup')
  expect_true(length(dbs) == 2)
  expect_equal(unname(sapply(dbs, function(x) x$database)), c('dup', 'dup'))
  res <- execute(DBM, 'dup', 'select count(*) from sqlite_master')
  expect_equal(as.numeric(unlist(res)), 2)
  res <- execute(DBM, 'dup', 'select count(*) from sqlite_master', index=2)
  expect_equal(as.numeric(unlist(res)), 3)
})

context('helpers')

test_that('it should test rac function', {
  DBM <- sqliter()
  got <- execute(DBM, 'test', 'select * from test1', post_proc=rac(,1))
  expect_equal(1:4, got)
  got <- execute(DBM, 'test', 'select * from test1', post_proc=rac(1))
  expect_equal(1, got$c1)
  expect_equal('a', got$c2)
  got <- execute(DBM, 'test', 'select * from test1', post_proc=rac(1,2))
  expect_equal('a', got)
})
