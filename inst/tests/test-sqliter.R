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
  expect_true(length(tbls) == 1)
  expect_true(tbls[[1]]$database == 'funds')
  expect_true(length(tbls[[1]]$entity_names) == 3)
  tbls <- tables(DBM, 'dup')
  expect_true(length(tbls) == 2)
  expect_true(length(tbls[[1]]$entity_names) == 2)
  expect_true(length(tbls[[2]]$entity_names) == 3)
})

context('execute query')

test_that('it should execute a query', {
  DBM <- sqliter(path='.')
  res <- query(DBM, 'funds', 'select count(*) from sqlite_master')
  expect_equal(as.numeric(unlist(res)), 3)
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
