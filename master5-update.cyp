
LOAD CSV WITH HEADERS FROM "file:///sub-US1.csv" AS line
with distinct line, split(line.`JOURNALS SUBSCRIPTIONS:START DATE`, '/') as startdate
MATCH(year:Year {year: toInt(startdate[2])})
MATCH (subscription:Subscription {seq: toInt(line.SEQ)})
MERGE (year)<-[:MADEIN]-(subscription)

