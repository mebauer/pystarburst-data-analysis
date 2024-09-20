import os
import trino
from pystarburst import Session
from pystarburst import functions as f
from pystarburst.functions import col, lag, round, row_number
from pystarburst.window import Window

host = str(os.getenv('host'))
port = os.getenv('port')
http_scheme = str(os.getenv('http_scheme'))
username = str(os.getenv('username'))
password = str(os.getenv('password'))

db_parameters = {
    "host": host,
    "port": port,
    "http_scheme": http_scheme,
    # Setup authentication through login or password or any other supported authentication methods
    # See docs: https://github.com/trinodb/trino-python-client#authentication-mechanisms
    "auth": trino.auth.BasicAuthentication(username, password)
}
session = Session.builder.configs(db_parameters).create()

print(type(session))

# Analyze Aviation Data

allFs = session.table("tmp_cat.aviation.raw_flight")
print(type(allFs))

print(allFs.count())

# get the whole table, aggregate & sort
mostAs = (
    session 
    .table("tmp_cat.aviation.raw_airport") 
    .group_by("country").count() 
    .sort("count", ascending=False)
)

mostAs.show()

# get the whole table, aggregate & sort
mostFs = (
    session 
    .table("tmp_cat.aviation.raw_flight") 
    .group_by("unique_carrier").count() 
    .rename("unique_carrier", "carr") 
    .sort("count", ascending=False)
)

mostFs.show(5)

# get all of the carriers
allCs = session.table("tmp_cat.aviation.raw_carrier")
 
# repurpose mostFs from above (or chain on it) 
#   to join the 2 DFs and sort the results that
#   have already been grouped
top5CarrNm = (
    mostFs 
    .join(allCs, mostFs.carr == allCs.code) 
    .drop("code") 
    .sort("count", ascending=False)
)

top5CarrNm.show(5, 30)

# trimFs are flights projected & filtered
trimFs = (
    session.table("tmp_cat.aviation.raw_flight") 
    .rename("tail_number", "tNbr") 
    .select("tNbr", "distance") 
    .filter(col("distance") > 1500) 
)
 
# trimPs are planes table projected & filtered
trimPs = (
    session.table("tmp_cat.aviation.raw_plane") 
    .select("tail_number", "model") 
    .filter("model is not null")
)
 
# join, group & sort
q5Answer = (
    trimFs 
    .join(trimPs, trimFs.tNbr == trimPs.tail_number) 
    .drop("tail_number") 
    .group_by("model").count() 
    .sort("count", ascending=False) 
)

q5Answer.show()

# temp DF holds counts for each originating airport 
#   by month
aggFlights = (
    session.table("tmp_cat.aviation.raw_flight") 
    .select("origination", "month") 
    .rename("origination", "orig") 
    .group_by("orig", "month").count() 
    .rename("count", "num_fs")
)

# define a window specification
w1 = Window.partition_by("orig").order_by("month")
 
# add col to grab the prior row's nbr flights
changeFlights = (
    aggFlights 
    .withColumn("num_fs_b4", 
        lag("num_fs",1).over(w1))
)

# add col for the percentage change
q6Answer = (
    changeFlights 
    .withColumn("perc_chg", 
        round((1.0 * (col("num_fs") - col("num_fs_b4")) / 
              (1.0 * col("num_fs_b4"))), 1))
)

q6Answer.show()

# determine counts from orig>dest pairs
popularRoutes = (
    session 
    .table("tmp_cat.aviation.raw_flight") 
    .rename("origination", "orig") 
    .rename("destination", "dest") 
    .group_by("orig", "dest").count() 
    .rename("count", "num_fs")
)

# define a window specification
w2 = (
    Window.partition_by("orig") 
    .order_by(col("num_fs").desc())
)
 
# add col to put the curr row's ranking in
rankedRoutes = (
    popularRoutes 
    .withColumn("rank", 
        row_number().over(w2))
)

# just show up to 3 for each orig airport
q7Answer = (
    rankedRoutes 
    .filter(col("rank") <= 3) 
    .sort("orig", "rank")
)

q7Answer.show(17);