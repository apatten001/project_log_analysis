#written with python==3.6.3

import psycopg2 as pg

DBNAME = 'news'

quest_1 = 'What are the most popular 3 articles of all time? \n'

quest_1_query = '''SELECT articles.title, count(*) as view_count
                            FROM articles INNER JOIN log ON log.path
                            LIKE concat('%',articles.slug,'%')
                            WHERE log.status LIKE '%200%'
                            GROUP BY articles.title, log.path
                            ORDER BY view_count DESC LIMIT 3;'''

quest_2 = 'Who are the most popular article authors of all time? \n'

quest_2_query = '''SELECT authors.name , count(*) as view_count
                        FROM articles LEFT JOIN log ON log.path
                        LIKE concat('%',articles.slug,'%')
                        LEFT JOIN authors ON authors.id = articles.author
                        GROUP BY authors.name
                        ORDER BY view_count DESC'''

quest_3 = 'On which days did more than 1% of requests lead to errors? \n'

quest_3_query = '''SELECT date(time), count(status) as error_count FROM log
                        WHERE status LIKE '%404%'
                        Group BY date(time)
                        ORDER BY error_count DESC 
                        LIMIT 1'''


def connect_database(dbname):
    # connects to the database specified
    return pg.connect(database=dbname)


def close_database():
    # closes the database
    db = connect_database(DBNAME)
    db.close()


def execute_quest_query(query):

    # set database as variable
    db = connect_database(DBNAME)

    # connect the cursor to the database
    cursor = db.cursor()

    # execute on the query
    cursor.execute(query)

    try:
        for _object, view in cursor.fetchall():
            print('{} -- {} views'.format(_object, view))
    except quest_3:
        for date, errors in cursor.fetchall():
            print('{} -- {} errors'.format(date, errors))
    close_database()


if __name__ == '__main__':
    print(quest_1)
    execute_quest_query(quest_1_query)
    print('\n' + quest_2)
    execute_quest_query(quest_2_query)
    print('\n' + quest_3)
    execute_quest_query(quest_3_query)



