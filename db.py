import psycopg2
from datetime import datetime

#####################
## DATABASE CONFIG ##
#####################

dbName = 'fundb'
dbUser = 'postgres'
dbPassword = ''
dbHost = '127.0.0.1'
dbTable = 't18'


# Set this variable to true to see logs in console when debugging
dbDebug = True


# Method to store data to database
def storeDataToDB(data, url):

    # Establish connection with database
    try:
        connection = psycopg2.connect(user=dbUser,
                                      password=dbPassword,
                                      host=dbHost,
                                      port="5433",
                                      database=dbName)

        cursor = connection.cursor()
        if dbDebug:
            print("Connected to DB")

        # Create table if does not exists
        try:
            cursor.execute("select exists(select relname from pg_class where relname='" + dbTable + "')")
            exists = cursor.fetchone()[0]

            if not exists:
                qry = """
                        CREATE TABLE """ + dbTable + """ (
                            id SERIAL PRIMARY KEY,
                            data VARCHAR(255) NOT NULL,
                            url VARCHAR(255) NOT NULL,
                            scrapped_at TIMESTAMP WITHOUT TIME ZONE NOT NULL
                        )
                        """

                cursor.execute(qry)
                connection.commit()
                if dbDebug:
                    print("Table Created")
        except Exception as e:
            connection.rollback()
            if dbDebug:
                print("Table already exists")
                print(str(e))

        # Insert data to table
        try:
            for rows in data:
                for key, value in rows.items():
                    # Data needs to be pre-processed
                    processedText = value.replace("\n","").replace("\t","")
                    print(processedText)
                    qry = """INSERT INTO """ + dbTable + """(data,url,scrapped_at)
                                 VALUES(%s,%s,%s) RETURNING id;"""
                    cursor.execute(qry, (processedText, url, datetime.now(),))
                    connection.commit()

        except Exception as e:
            connection.rollback()
            if dbDebug:
                print("Error in inserting")
                print(str(e))

        cursor.close()

    except Exception as error:
        if dbDebug:
            print("Connection Error: ", error)
        else:
            pass



# Read all data from db
def readAllDataFromDB():
    try:
        connection = psycopg2.connect(user=dbUser,
                                      password=dbPassword,
                                      host=dbHost,
                                      port="5432",
                                      database=dbName)

        cursor = connection.cursor()
        if dbDebug:
            print("Connected to DB")

        try:
            cursor.execute("SELECT id, data, url, scrapped_at from " + dbTable)
            rows = cursor.fetchall()
            for r in rows:
                print(r)
        except Exception as e:
            if dbDebug:
                print('cant read')
                print(str(e))
            else:
                pass

    except Exception as error:
        if dbDebug:
            print("Connection Error: ", error)
        else:
            pass


# Read all data from db for specific url
def readUrlDataFromDB(url):
    try:
        connection = psycopg2.connect(user=dbUser,
                                      password=dbPassword,
                                      host=dbHost,
                                      port="5432",
                                      database=dbName)

        cursor = connection.cursor()
        if dbDebug:
            print("Connected to DB")

        try:
            cursor.execute("SELECT id, data, url, scrapped_at from " + dbTable + " where url = '" + url + "'")
            rows = cursor.fetchall()
            for r in rows:
                print(r)
        except Exception as e:
            if dbDebug:
                print('cant read')
                print(str(e))
            else:
                pass

    except Exception as error:
        if dbDebug:
            print("Connection Error: ", error)
        else:
            pass

readAllDataFromDB()
# readUrlDataFromDB("funtweets.com")



