## Contex manager
How to use the connection context manager

A Connection object can be used as a context manager that automatically commits or rolls back open transactions when leaving the body of the context manager. If the body of the with statement finishes without exceptions, the transaction is committed. If this commit fails, or if the body of the with statement raises an uncaught exception, the transaction is rolled back. If autocommit is False, a new transaction is implicitly opened after committing or rolling back.

If there is no open transaction upon leaving the body of the with statement, or if autocommit is True, the context manager does nothing.

Note

The context manager neither implicitly opens a new transaction nor closes the connection. If you need a closing context manager, consider using contextlib.closing().

con = sqlite3.connect(":memory:")
con.execute("CREATE TABLE lang(id INTEGER PRIMARY KEY, name VARCHAR UNIQUE)")

# Successful, con.commit() is called automatically afterwards
with con:
    con.execute("INSERT INTO lang(name) VALUES(?)", ("Python",))

# con.rollback() is called after the with block finishes with an exception,
# the exception is still raised and must be caught
try:
    with con:
        con.execute("INSERT INTO lang(name) VALUES(?)", ("Python",))
except sqlite3.IntegrityError:
    print("couldn't add Python twice")

# Connection object used as context manager only commits or rollbacks transactions,
# so the connection object should be closed manually
con.close()

