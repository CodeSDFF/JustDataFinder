#SQL Data Finder
#python and the psycopg2 library
from contextlib import contextmanager
#This function is a decorator that can be used to define a factory function for with statement context managers, without needing to create a class or separate __enter__() and __exit__() methods.
import psycopg2

@contextmanager
def db_session_context(db_name):
    try:
        db = psycopg2.connect(database=db_name)
        yield db
    finally:
        db.close()

DB_NAME = 'news'

# Question 1
	
def most_popular_articles():
    """What are the most popular three articles of all time?"""

    print "\n\n ***** Three most popular articles of all time ***** "

    with db_session_context(DB_NAME) as db:
		
        cursor = db.cursor()
        cursor.execute("""
            SELECT articles.title, count(log.status) as count
            FROM articles, log
            WHERE log.status = '200 OK'
              and log.path like '%' || articles.slug || '%'
            GROUP BY articles.title
            ORDER BY count desc
            LIMIT 3;
        """)
        results = cursor.fetchall()

    # for each article/view count tuple, print article -- view count
    for article in results:
        print "     %s: %s views" % (article[0], article[1])
		
# Question 2

def most_popular_authors():
    """Who are the most popular article authors of all time?"""

    print "\n\n ***** Most popular authors of all time ***** "

    with db_session_context(DB_NAME) as db:
        cursor = db.cursor()
        cursor.execute("""
            SELECT authors.name, count(log.status) as count
            FROM authors, articles, log
            WHERE articles.author = authors.id
              and log.status = '200 OK'
              and log.path like '%' || articles.slug || '%'
            GROUP BY authors.name
            ORDER BY count desc;
        """)
        results = cursor.fetchall()

    # For each author/view count tuple, print author -- view count
    for author in results:
        print "     %s: %s views" % (author[0], author[1])
	
 # Question 3

def get_errors():
		
    """On which days did more than 1% of requests lead to errors"""
	
	
	
    with db_session_context(DB_NAME) as db:
		c = db.cursor()
		c.execute("SELECT to_char(date, 'FMMonth FMDD, YYYY'), err/total as ratio"
				  " from (select time::date as date, "
				  "count(*) as total, "
				  "sum((status != '200 OK')::int)::float as err "
				  "from log "
				  "group by date) as errors "
				  "where err/total > .01;")
		errors = c.fetchall()
    	
    for i in errors:
	
        print "\n\n ***** More than 1% of requests lead to errors ***** ", i[0]
		
        print (str(round((i[1]*100), 2)) + '% errors')
        print ("---------------")
		
# Call all the functions if file is executed with the interpreter
if __name__ == "__main__":
    most_popular_articles()
    most_popular_authors()
    get_errors()
	

