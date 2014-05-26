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
	expect_equal(list_databases(DBM), c('funds', 'test'))
})

test_that('it should list databases using filter', {
	DBM <- sqliter(path='.')
	expect_equal(list_databases(DBM, 'fu'), 'funds')
})

context('execute query')

test_that('it should execute a query', {
	DBM <- sqliter(path='.')
	res <- execute(DBM, 'funds', 'select count(*) from sqlite_master')
	expect_equal(as.numeric(unlist(res)), 40)
})


