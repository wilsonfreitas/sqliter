# sqliter

<!-- When you collect data from many sources to get data explored it is common to
handle with many files at the same time.
I'd rather work with SQLite files just because I am able to get structed data
without much effort and I can get fresh updates without much pain.
However, the  -->

**sqliter** takes out the misery of your life helping you with SQLite
queries without turing your code into a mess.
Once you have many SQLite files to handle it simplifies your query calls by
structuring the way you connect to your files, the databases.

Let's suppose you have some SQLite files spread on your computer.
Some files you are working on now, and some others you have worked in other 
projects before.
The standard approach would be copy the files into your new project or point 
to the files wherever they are.
For those files you would simply create their connections, execute
your queries creating a brand new `data.frame` and transform your data to
get your job done.

When you have many files, you find yourself creating a bunch of
annoying repeated code.
I don't like repeated code, it smells bad and
I take seriously the DRY principle.
That is the reason why I created **sqliter**, to take away this misery of my life.

In order to use **sqliter** you must declare the path where your SQLite files
are hidden.

	DBM <- sqliter(path=c('data', '../project2/data', '/path/to/project3/data'))

and query the databases.

	ds <- DBM$query_database_dummy('select count(*) from dummytable')

where `database_dummy` is the name of SQLite file, without extension, lying
inside some directory declared in the `path`.
So, it should stand for `data/database_dummy.db`, `../project2/data/database_dummy.db` or `/path/to/project3/data/database_dummy.db`.
The returned object, `ds`, is a `data.frame` with a column named `count(*)`.
The column names can be manipulated like any other sql call, appending a label after the variable.

> Warning: **sqliter** is deeply intended to research purposes. I understand that
> by no means it should be used in any kind of production code.

