CREATE CONSTRAINT ON (journal:Journal) ASSERT journal.mnemonic IS UNIQUE;
CREATE CONSTRAINT ON (author:Author) ASSERT author.author_name IS UNIQUE;
CREATE CONSTRAINT ON (article:Article) ASSERT article.articleid IS UNIQUE;

LOAD CSV WITH HEADERS FROM "file:///Users/gf247/Documents/Dropbox/NEO4J-TEST/CUP-try/authordata3.csv" AS csvLine
MERGE (author:Author {author_name: UPPER(csvLine.author_name)})
MERGE (article:Article {articleid:toInt(csvLine.articleid), article_title:UPPER(csvLine.article_title)})
MERGE (journal:Journal {mnemonic: csvLine.mnemonic, journal_title:csvLine.journal_title})


LOAD CSV WITH HEADERS FROM "file:///Users/gf247/Documents/Dropbox/NEO4J-TEST/CUP-try/authordata3.csv" AS csvLine
MATCH (author:Author {author_name: UPPER(csvLine.author_name)})
MATCH (article:Article {articleid:toInt(csvLine.articleid), article_title:UPPER(csvLine.article_title)})
MATCH(journal:Journal {mnemonic: csvLine.mnemonic, journal_title:csvLine.journal_title})

MERGE (author)-[:AUTHORED_BY]->(article)
MERGE (article)-[:PUBLISHED_IN]->(journal)
MERGE (author)-[:AUTHORED_BY]->(journal)

CREATE CONSTRAINT ON (year:Year) ASSERT year.year IS UNIQUE;

LOAD CSV WITH HEADERS FROM "file:///Users/gf247/Documents/Dropbox/NEO4J-TEST/CUP-try/journals3.csv" AS csvLine
MATCH(journal:Journal {mnemonic: csvLine.mnemonic})
MERGE (year:Year {year:toInt(csvLine.year)})
MERGE (journal)-[:PUBLISHED]->(year)

LOAD CSV WITH HEADERS FROM "file:///Users/gf247/Documents/Dropbox/NEO4J-TEST/CUP-try/articledateV1.csv" AS csvLine
MATCH (article:Article {articleid:toInt(csvLine.articleid)})
MATCH (year:Year {year:toInt(csvLine.year)})
MERGE (article)-[:PUBLISHED]->(year)

CREATE CONSTRAINT ON (customer:Customer) ASSERT customer.customerid IS UNIQUE;

LOAD CSV WITH HEADERS FROM "file:///Users/gf247/Documents/Dropbox/NEO4J-TEST/CUP-try/customer-data-name-NB-3.csv" AS csvLine
MERGE (customer:Customer {customerid: toInt(csvLine.customerid), customer_name: UPPER(csvLine.customer_name), customer_brick: UPPER(csvLine.customer_brick)})

LOAD CSV WITH HEADERS FROM "file:///Users/gf247/Documents/Dropbox/NEO4J-TEST/CUP-try/mnemonic-userid.csv" AS csvLine
MATCH (customer:Customer {customerid: toInt(csvLine.customerid)})
MATCH (journal:Journal {mnemonic: csvLine.mnemonic})
MERGE (customer)-[:BOUGHT]->(journal)

CREATE CONSTRAINT ON (affiliation:Affiliation) ASSERT affiliation.affiliation IS UNIQUE;
LOAD CSV WITH HEADERS FROM "file:///Users/gf247/Documents/Dropbox/NEO4J-TEST/CUP-try/articleaffiliation1.csv" AS csvLine
MATCH (article:Article {articleid:toInt(csvLine.articleid)})
MATCH (author:Author {author_name: UPPER(csvLine.author_name)})
MERGE (affiliation:Affiliation {affiliation: UPPER(csvLine.affiliation)})
MERGE (article)-[:PUBLISHED_BY]->(affiliation)
MERGE (author)-[:AFFILIATED_TO]->(affiliation)

//query trial

MATCH (article:Article)-[:PUBLISHED_IN]->(journal:Journal)
MATCH (journal)-[:PUBLISHED]->(year:Year)
MATCH (article)-[:PUBLISHED]->(year)
MATCH (affiliation:Affiliation)<-[:PUBLISHED_BY]-(article)
MATCH (customer:Customer)-[:BOUGHT]->(journal)
WHERE article.article_title CONTAINS 'ALZHEIM' AND year.year > 2015 AND journal.journal_title CONTAINS 'Agein' AND customer.customer_name CONTAINS 'ROMA'
RETURN journal,article,year,customer,affiliation;

// match customer and affiliation 

MATCH (article:Article)-[:PUBLISHED_IN]->(journal:Journal)
MATCH (journal)-[:PUBLISHED]->(year:Year)
MATCH (article)-[:PUBLISHED]->(year)
MATCH (affiliation:Affiliation)<-[:PUBLISHED_BY]-(article)
MATCH (customer:Customer)-[:BOUGHT]->(journal)
WHERE  affiliation.affiliation CONTAINS AND customer.customer_name CONTAINS 'ROMA'
RETURN journal,article,year,customer,affiliation;

// subscriptions

CREATE CONSTRAINT ON (subscription:Subscription) ASSERT subscription.sub_pin IS UNIQUE;
DROP CONSTRAINT ON (subscription:Subscription) ASSERT subscription.sub_pin IS UNIQUE;

USING PERIODIC COMMIT 10000
LOAD CSV WITH HEADERS FROM "file:///Users/gf247/Documents/Dropbox/NEO4J-TEST/CUP-try/cup003b_subs_data-gfV2.csv" AS csvLine
MERGE (subscription:Subscription {sub_pin:toInt(csvLine.SUB_PIN), sub_paid_value:toInt(csvLine.SUB_PAID_VALUE), sub_currency_code:UPPER(csvLine.SUB_CURRENCY_CODE)})






