import petl as etl, psycopg2 as pg, pymysql as mysql

dbConfig = {
    "source": "dbname=beta user=postgres password=pg host=localhost", 
    "staging": "dbname=test user=postgres password=pg host=localhost",
    "data_warehouse": { 
        "host": "localhost",
        "user": "root",
        "password": "",
        "database": "beta"
    }
}

SOURCE_DB_DBO = pg.connect(dbConfig["source"])

STAGING_DB_DBO = pg.connect(dbConfig["source"])

DW_DB_DBO = mysql.connect(
  host= dbConfig["data_warehouse"]["host"],
  user= dbConfig["data_warehouse"]["user"],
  password= dbConfig["data_warehouse"]["password"],
  database= dbConfig["data_warehouse"]["database"]
)

STAGING_DB_DBO.cursor().execute('DROP TABLE IF EXISTS users;')

source_users_table = etl.fromdb(SOURCE_DB_DBO, 'SELECT userid, username, email, role  FROM tuser;')

#petl.io.db.todb(table, dbo, tablename, schema=None, commit=True, create=False, drop=False, constraints=True, metadata=None, dialect=None, sample=1000)
etl.todb(source_users_table, STAGING_DB_DBO, "users", create= True)

staging_users_table = etl.fromdb(STAGING_DB_DBO, 'SELECT username, email FROM users;')

#for MySQL the statement SET SQL_MODE=ANSI_QUOTES is required to ensure MySQL uses SQL-92 standard quote characters.
DW_DB_DBO.cursor().execute('SET SQL_MODE=ANSI_QUOTES')

#table should be exists in db for append data.
etl.appenddb(staging_users_table, DW_DB_DBO, "user")

#etl.todb(staging_users_table, DW_DB_DBO, "user", create= True)


