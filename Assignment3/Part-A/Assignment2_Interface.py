#
# Assignment2 Interface
#

import psycopg2
import sys
from threading import Thread

# Do not close the connection inside this file i.e. do not perform openConnection.close()
def spatialFragments (openConnection):
    cur = openConnection.cursor()

    cur.execute('select max(st_ymax(geom) - st_ymin(geom)) from rectangles')
    largest_height = cur.fetchone()[0]
    fragment_height = largest_height / 2

    cur.execute('select * from rectangles order by latitude2 desc limit 1')
    highest_latitude = cur.fetchone()[3]

    cur.execute('select * from rectangles order by latitude1 asc limit 1')
    lowest_latitude = cur.fetchone()[1]
    fragment_point = (highest_latitude - lowest_latitude) / 4

    for i in range(4):
     s = str(i+1)
     cur.execute(
        f'drop table if exists pointsTable_f{s};'
        f'drop table if exists rectsTable_f{s};'
     )

    rectsTableFragments(fragment_height, fragment_point, lowest_latitude, cur)
    pointsTableFragments(fragment_height, fragment_point, lowest_latitude, cur)

    print('fragments complete')
    cur.close()
    openConnection.commit()
    pass

def rectsTableFragments(fragment_height, fragment_point, lowest_latitude, cur):
    cur.execute('CREATE TABLE rectsTable_f1 AS select * from rectangles where latitude1 >= ' + str(lowest_latitude)
    + 'and latitude1 <= ' + str(fragment_point))
    cur.execute('CREATE TABLE rectsTable_f2 AS select * from rectangles where latitude1 <= ' + str(fragment_height))
    cur.execute('CREATE TABLE rectsTable_f3 AS select * from rectangles where latitude1 >= ' + str(fragment_height))
    cur.execute('CREATE TABLE rectsTable_f4 AS select * from rectangles where latitude1 >= ' + str(fragment_point)
    + 'and latitude1 <= ' + str(fragment_height))

def pointsTableFragments(fragment_height, fragment_point, lowest_latitude, cur):
    cur.execute('CREATE TABLE pointsTable_f1 AS select * from points where latitude >= ' + str(lowest_latitude)
    + 'and latitude <= ' + str(fragment_point))
    cur.execute('CREATE TABLE pointsTable_f2 AS select * from points where latitude <= ' + str(fragment_height))
    cur.execute('CREATE TABLE pointsTable_f3 AS select * from points where latitude >= ' + str(fragment_height))
    cur.execute('CREATE TABLE pointsTable_f4 AS select * from points where latitude >= ' + str(fragment_point)
     + 'and latitude <= ' + str(fragment_height))

def parallelJoin (pointsTable, rectsTable, outputTable, outputPath, openConnection):
    spatialFragments(openConnection)
    cur = openConnection.cursor()
    cur.execute(f"drop table if exists {outputTable};")
    cur.execute("CREATE TABLE " + outputTable + " (points_count bigint, rectangle geometry)")

    list=[]
    for i in range(4):
         thread = Thread(target = joinFragments, args=('pointsTable_f' + str(i+1),'rectsTable_f' + str(i+1), outputTable, cur))
         list.append(thread)

    for i in range(4):
        list[i].start()

    for i in range(4):
        list[i].join()

    cur.execute(f"SELECT DISTINCT points_count, rectangle from {outputTable} order by points_count asc")
    rows = cur.fetchall()
    f = open(outputPath, "a")
    for row in rows:
        f.write(str(row[0])+'\n')
    f.close()
    print('join done')
    openConnection.commit()

def joinFragments(pointsTable, rectsTable, outputTable, cur):
    cur.execute('INSERT INTO ' + outputTable + ' (points_count,rectangle) SELECT  count( ' + pointsTable + '.geom) AS count , ' + rectsTable
        +'.geom  as rectangle FROM ' + rectsTable
        + ' JOIN ' + pointsTable + ' ON st_contains('+ rectsTable +'.geom,' + pointsTable + '.geom) GROUP BY '
        +rectsTable +'.geom order by count asc')

################### DO NOT CHANGE ANYTHING BELOW THIS #############################

# Donot change this function
def getOpenConnection(user='postgres', password='admin', dbname='dds_assignment2'):
    return psycopg2.connect("dbname='" + dbname + "' user='" + user + "' host='localhost' password='" + password + "'")

# Donot change this function
def createDB(dbname='dds_assignment2'):
    """
    We create a DB by connecting to the default user and database of Postgres
    The function first checks if an existing database exists for a given name, else creates it.
    :return:None
    """
    # Connect to the default database
    con = getOpenConnection(dbname='postgres')
    con.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT)
    cur = con.cursor()

    # Check if an existing database with the same name exists
    cur.execute('SELECT COUNT(*) FROM pg_catalog.pg_database WHERE datname=\'%s\'' % (dbname,))
    count = cur.fetchone()[0]
    if count == 0:
        cur.execute('CREATE DATABASE %s' % (dbname,))  # Create the database
    else:
        print('A database named {0} already exists'.format(dbname))

    # Clean up
    cur.close()
    con.commit()
    con.close()

# Donot change this function
def deleteTables(tablename, openconnection):
    try:
        cursor = openconnection.cursor()
        if tablename.upper() == 'ALL':
            cursor.execute("SELECT table_name FROM information_schema.tables WHERE table_schema = 'public'")
            tables = cursor.fetchall()
            for table_name in tables:
                cursor.execute('DROP TABLE %s CASCADE' % (table_name[0]))
        else:
            cursor.execute('DROP TABLE %s CASCADE' % (tablename))
        openconnection.commit()
    except psycopg2.DatabaseError as e:
        if openconnection:
            openconnection.rollback()
        print('Error %s' % e)
        sys.exit(1)
    except IOError as e:
        if openconnection:
            openconnection.rollback()
        print('Error %s' % e)
        sys.exit(1)
    finally:
        if cursor:
            cursor.close()