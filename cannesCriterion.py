import os
import csv
import re
import sqliteHelpers
from dotenv import load_dotenv

# data structurs <- 09/21/25 16:24:16 # 
criteriondirectors = set()
cannesdirectors = set()

# getting the moviedatabase connection <- 09/21/25 16:24:09 # 
con = sqliteHelpers.MDCon()

# NOTES: the select function always returns a list of tuples <- 09/21/25 16:06:41 # 

# reading criterion directors into directornames <- 09/21/25 16:07:10 # 
with open(os.getenv("__MOVIE_DATABASE_PATH", ".") + "/criterion.tsv", "r") as rdob:
    reader = csv.reader(rdob, delimiter="\t")
    for readrow in reader: 
        dirname = re.sub(r"[^a-z]", "%", readrow[2].lower().strip())
        criteriondirectors.add(dirname) if dirname not in criteriondirectors else next

# reading cannes directors into directornames<- 09/21/25 16:07:14 # 
with open(os.getenv("__MOVIE_DATABASE_PATH", ".") + "/cannes.tsv", "r") as rdob:
    reader = csv.reader(rdob, delimiter="\t")
    for readrow in reader: 
        dirname = re.sub(r"[^a-z]", "%", readrow[1].lower().strip())
        cannesdirectors.add(dirname.lower().strip()) if dirname not in cannesdirectors else next

print("Done reading directors into data!")

def select_and_insert(directors: set[str], tablename: str):
    # counter for status <- 09/21/25 16:25:32 # 
    count = 0

    for name in directors:
        filmtupsdummy = []
        try:
            filmtupsdummy = con.execute("SELECT tconst FROM Directors where nconst in (SELECT nconst FROM Names WHERE name LIKE ?)", (sqliteHelpers.wildcardWrapForLIKE(name),)).fetchall()
        except Exception as e:
            print(f"Error with select: {e}")
        print("Trying insert...")
        for x in filmtupsdummy:
            print(x[0])
            try: 
                sqliteHelpers.insert(con.cur(),f"{tablename}",tconst=x[0])
                con.commit()
            except Exception as e:
                print(f"Error with insert: {e}")
        count += 1
        print(str(round(count/len(criteriondirectors)*100,1)) + "%")

select_and_insert(cannesdirectors, "Cannes")
select_and_insert(criteriondirectors, "Criterion")
