from diracinfosites import main
# to do:
# implement sql query

#conn = MySQLdb.connect(
#    db='test', username='ianb')
#import MySQLdb # NOT PART OF DIRAC!

def read_sql_status(ce,tag):
    # returns good or bad
    # sql query
    sql_query = "SELECT status FROM mytable WHERE (ce='%s' AND tag='%s')"%(ce,tag)
    # should just return 1 row
    # dummy
    import random
    number = random.randrange(-10,10)
    if number<=0:
        return 'bad'
    else:
        return 'good'

def insert_record(site,ce,tag):
    sql_query = "INSERT mytable SET (site=%s, ce=%s, tag=%s)"%(site,ce,tag)
    return sql_query

def remove_record(site,ce,tag):
    sql_query = "SELECT index FROM mytable WHERE (ce=%s, site=%s, tag=%s)"%(site,ce,tag)
    return sql_query

results = main("glast.org") # this one does all the content creation!

print('SITE\tCE\tTag\tstatus')
#i = 0
bdii_records = []

for site in results:
    for ce in results[site]["CE"]:
        for tag in results[site]["Tags"]:
            bdii_records+=["%s\t%s\t%s"%(site,ce,tag)]
            # #print '%s\t%s\t%s\tgood'%(site,ce,tag)
            # #status = read_sql_status(ce,tag)
            # #if not status == 'bad':
            # sql_query=update_sql_entry(site,ce,tag)
            # print 'issue: %s'%sql_query
            # #print '%i: %s\t%s\t%s\t%s'%(i,site,ce,tag,status)
            # #i+=1

# now need to check table
db_records = do_sqlquery("SELECT ce, site, tag FROM mytable WHERE status='good'")
db_records_bad = do_sqlquery("SELECT ce, site, tag FROM mytable WHERE status='bad'")

# do the rest only for "good" lines
for record in db_records:
    if record in bdii_records: 
        update(record) # update the record
    else:
        remove_record(record) # bdii does not contain db entry, tag removed?

for record in bdii_records:
    if not (record in db_records) or (record in db_records_bad):
        # there is a new record of a new tag, insert in the DB
        insert_record(record)
    
