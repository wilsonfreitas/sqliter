context('create sqliter')

test_that('it should create sqliter', {
	DBM <- sqliter(path='.')
	expect_is(DBM, 'sqliter')
	expect_equal(DBM$get('path'), '.')
})

context('execute query')

test_that('it should execute a query', {
	DBM <- sqliter(path='.')
	res <- execute(DBM, 'funds', 'select count(*) from sqlite_master')
	expect_equal(as.numeric(unlist(res)), 40)
})

