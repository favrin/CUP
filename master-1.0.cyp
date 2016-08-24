CREATE CONSTRAINT ON (journal:Journal) ASSERT journal.mnemonic IS UNIQUE;
CREATE CONSTRAINT ON (author:Author) ASSERT author.author_name IS UNIQUE;
CREATE CONSTRAINT ON (article:Article) ASSERT article.articleid IS UNIQUE;


USING PERIODIC COMMIT 5000
LOAD CSV WITH HEADERS FROM "file:///authordata3.csv" AS csvLine
MERGE (author:Author {author_name: UPPER(csvLine.author_name)})
MERGE (article:Article {articleid:toInt(csvLine.articleid), article_title:UPPER(csvLine.article_title)})
MERGE (journal:Journal {mnemonic: csvLine.mnemonic, journal_title:UPPER(csvLine.journal_title)})


LOAD CSV WITH HEADERS FROM "file:///authordata3.csv" AS csvLine
MATCH (author:Author {author_name: UPPER(csvLine.author_name)})
MATCH (article:Article {articleid:toInt(csvLine.articleid), article_title:UPPER(csvLine.article_title)})
MATCH(journal:Journal {mnemonic: csvLine.mnemonic, journal_title:UPPER(csvLine.journal_title)})

MERGE (author)-[:AUTHORED_BY]->(article)
MERGE (article)-[:PUBLISHED_IN]->(journal)
MERGE (author)-[:AUTHORED_BY]->(journal)

CREATE CONSTRAINT ON (year:Year) ASSERT year.year IS UNIQUE;

LOAD CSV WITH HEADERS FROM "file:///journals3.csv" AS csvLine
MATCH(journal:Journal {mnemonic: csvLine.mnemonic})
MERGE (year:Year {year:toInt(csvLine.year)})
MERGE (journal)-[:PUBLISHED]->(year)

LOAD CSV WITH HEADERS FROM "file:///articledateV1.csv" AS csvLine
MATCH (article:Article {articleid:toInt(csvLine.articleid)})
MATCH (year:Year {year:toInt(csvLine.year)})
MERGE (article)-[:PUBLISHED]->(year)

CREATE CONSTRAINT ON (customer:Customer) ASSERT customer.customerid IS UNIQUE;

LOAD CSV WITH HEADERS FROM "file:///customer-data-name-NB-3.csv" AS csvLine
MERGE (customer:Customer {customerid: toInt(csvLine.customerid), customer_name: UPPER(csvLine.customer_name), customer_brick: UPPER(csvLine.customer_brick)})

USING PERIODIC COMMIT 5000
LOAD CSV WITH HEADERS FROM "file:///mnemonic-userid.csv" AS csvLine
MATCH (customer:Customer {customerid: toInt(csvLine.customerid)})
MATCH (journal:Journal {mnemonic: csvLine.mnemonic})
MERGE (customer)-[:BOUGHT]->(journal)

CREATE CONSTRAINT ON (affiliation:Affiliation) ASSERT affiliation.affiliation IS UNIQUE;

USING PERIODIC COMMIT 5000
LOAD CSV WITH HEADERS FROM "file:///articleaffiliation1.csv" AS csvLine
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
WHERE article.article_title CONTAINS 'ALZHEIM' AND year.year > 2015 AND journal.journal_title CONTAINS 'AGE' AND customer.customer_name CONTAINS 'ROMA'
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
CREATE CONSTRAINT ON (subscription:Affiliation) ASSERT affiliation.affiliation IS UNIQUE;



CREATE CONSTRAINT ON (isbn:Isbn) ASSERT isbn.isbn10 IS UNIQUE;

USING PERIODIC COMMIT 5000 
LOAD CSV WITH HEADERS FROM "file:///bookNV3.txt" AS csvLine
MERGE (isbn:Isbn {isbn10:csvLine.ISBN10, isbn13:csvLine.ISBN13,format:UPPER(csvLine.FORMAT_LEGEND), title_full:UPPER(csvLine.TITLE_FULL)})

USING PERIODIC COMMIT 5000 
LOAD CSV WITH HEADERS FROM "file:///authorNV1.txt" AS csvLine
MATCH (isbn:Isbn {isbn10:csvLine.ISBN10})
SET isbn.author_singleline=UPPER(csvLine.AUTHOR_SINGLELINE)



CREATE CONSTRAINT ON (book:Book) ASSERT book.title_code IS UNIQUE;

USING PERIODIC COMMIT 10000
LOAD CSV WITH HEADERS FROM "file:///title_code.txt" AS csvLine
MATCH (isbn:Isbn {isbn10:csvLine.ISBN10})
MERGE (book:Book {title_code:toInt(csvLine.TITLE_CODE)})
MERGE (book)-[:HAS]->(isbn)

USING PERIODIC COMMIT 5000 
LOAD CSV WITH HEADERS FROM "file:///books_product-dateV2NB.csv" AS csvLine
WITH distinct csvLine, SPLIT(csvLine.PUBLICATION_DATE, '/') AS date
MATCH (isbn:Isbn {isbn10:csvLine.ISBN10})
SET isbn.year=date[2]

MERGE (isbn:Isbn) -[:ISBN_PUBLISHED]->(year:Year)
WHERE isbn.year = year.year

USING PERIODIC COMMIT 5000 
LOAD CSV WITH HEADERS FROM "file:///date-book.csv" AS csvLine
MATCH (isbn:Isbn {isbn10:csvLine.ISBN10})
MATCH (year:Year {year:toInt(csvLine.year)})
MERGE (isbn)-[:ISBN_PUBLISHED]->(year)


USING PERIODIC COMMIT 5000 
LOAD CSV WITH HEADERS FROM "file:///books_product-dateV2NB.csv" AS csvLine
MATCH (isbn:Isbn {isbn10:csvLine.ISBN10})
SET isbn.subject=UPPER(csvLine.IMPRINT_LEGEND)


CREATE CONSTRAINT ON (country:Country) ASSERT country.brik IS UNIQUE;


USING PERIODIC COMMIT 5000 
LOAD CSV WITH HEADERS FROM "file:///customer-data-name-NB-3.csv" AS csvLine
MATCH (customer:Customer {customerid: toInt(csvLine.customerid)})
MERGE (country:Country {brick: UPPER(csvLine.customer_brick)})
MERGE (customer)-[:RESIDES]->(country)

MATCH (country:Country)
WHERE country.brick= 'ENGLAND'

RETURN country;

LOAD CSV WITH HEADERS FROM "file:///CUSTOMERS.txt" AS csvLine
WITH csvLine
LIMIT 1
RETURN csvLine;

/// customers books 
CREATE CONSTRAINT ON (b_customer:B_Customer) ASSERT b_customer.cup_account_n IS UNIQUE;

USING PERIODIC COMMIT 5000 
LOAD CSV WITH HEADERS FROM "file:///CUSTOMER3.txt" AS csvLine
MERGE (b_customer:B_Customer {cup_account_n: toInt(csvLine.CUP_ACCOUNT_NUMBER_BK), b_customer_name:UPPER(csvLine.CUSTOMER_NAME), b_customer_brik:UPPER(csvLine.CUSTOMER_BRICK_LEGEND)})


USING PERIODIC COMMIT 5000 
LOAD CSV WITH HEADERS FROM "file:///TRANSACTION_JAN_16b.txt" AS csvLine
WITH distinct csvLine, SPLIT(csvLine.DOC_DATE, '/') AS date
MATCH (isbn:Isbn {isbn10:csvLine.ISBN10_CODE})
MATCH (b_customer:B_Customer {cup_account_n: toInt(csvLine.CUP_ACCOUNT_NUMBER) })
MERGE (isbn) -[r:B_BOUGHT]-> (b_customer)
SET r.year = toInt(date[2])
SET r.month = toInt(date[1])
SET r.day = toInt(date[0])


USING PERIODIC COMMIT 5000 
LOAD CSV WITH HEADERS FROM "file:///TRANSACTIONS-FEB-MAY-16.txt" AS csvLine
WITH distinct csvLine, SPLIT(csvLine.DOC_DATE, '/') AS date
MATCH (isbn:Isbn {isbn10:csvLine.ISBN10_CODE})
MATCH (b_customer:B_Customer {cup_account_n: toInt(csvLine.CUP_ACCOUNT_NUMBER) })
MERGE (isbn) -[r:B_BOUGHT]-> (b_customer)
SET r.year = toInt(date[2])
SET r.month = toInt(date[1])
SET r.day = toInt(date[0])

USING PERIODIC COMMIT 5000 
LOAD CSV WITH HEADERS FROM "file:///customer-data-name-NB-3.csv" AS csvLine
MATCH (country:Country {brick: UPPER(csvLine.customer_brick)})
DETACH DELETE brick


USING PERIODIC COMMIT 5000 
LOAD CSV WITH HEADERS FROM "file:///customer-data-name-NB-3.csv" AS csvLine
MATCH (customer:Customer {customerid: toInt(csvLine.customerid)})
MERGE (country:Country {brik: UPPER(csvLine.customer_brick)})
MERGE (customer)-[:RESIDES]->(country)


LOAD CSV WITH HEADERS FROM "file:///CUSTOMER_TYPE2.csv" AS csvLine
WITH csvLine
LIMIT 1
RETURN csvLine;


USING PERIODIC COMMIT 5000 
LOAD CSV WITH HEADERS FROM "file:///CUSTOMER_TYPE2.csv" AS csvLine
MERGE (b_customer:B_Customer {cup_account_n: toInt(csvLine.CUP_ACCOUNT_NUMBER_BK)})
SET b_customer.type =UPPER(csvLine.DESTINATION_TYPE_LEGEND)

USING PERIODIC COMMIT 5000 
LOAD CSV WITH HEADERS FROM "file:///CUSTOMER_TOWN.csv" AS csvLine
MERGE (b_customer:B_Customer {cup_account_n: toInt(csvLine.CUP_ACCOUNT_NUMBER_BK)})
SET b_customer.town =UPPER(csvLine.TOWN)

LOAD CSV WITH HEADERS FROM "file:///world-universities1.csv" AS csvLine
WITH csvLine
LIMIT 1
RETURN csvLine;



//Universities
CREATE CONSTRAINT ON (university:University) ASSERT university.uni_id IS UNIQUE;

USING PERIODIC COMMIT 5000 
LOAD CSV WITH HEADERS FROM "file:///world-universities1.csv" AS csvLine
MERGE (university:University {uni_name: UPPER(csvLine.NAME), uni_country_code: UPPER(csvLine.COUNTRY), uni_id:toInt(csvLine.ID)})

USING PERIODIC COMMIT 5000 
LOAD CSV WITH HEADERS FROM "file:///world-universities1.csv" AS csvLine
MATCH (university:University {uni_id:toInt(csvLine.ID)})
SET university.web=csvLine.WEB 

CREATE (institutions:Institutions {name:'INSTITUTION'})

MATCH (n:University)
MATCH (m:Institution)
MERGE (m)-[r:TYPE]->(n)
SET r.detail='UNIVERSITY'

CREATE (amazon:Amazon {name:'AMAZON'})

MATCH (n:Amazon)
MATCH (m:Institution)
MERGE (m)-[r:TYPE]->(n)
SET r.detail='AMAZON'

CREATE (school:School {name:'SCHOOL'})

MATCH (n:School)
MATCH (m:Institution)
MERGE (m)-[r:TYPE]->(n)
SET r.detail='SCHOOL'

CREATE (agent:Agent {name:'AGENT'})

MATCH (n:Agent)
MATCH (m:Institution)
MERGE (m)-[r:TYPE]->(n)
SET r.detail='AGENT'


MATCH (n:University)
MATCH (m:Institution)
MATCH (m)-[r:TYPE]->(n)
WHERE r.detail='UNIVERSITY'
DELETE r



CREATE (universities:Universities {name:'UNIVERSITIES'})

MATCH (n:Universities)
MATCH (m:Institution)
MERGE (m)-[r:TYPE]->(n)
SET r.detail='UNIVERSITIES'


MATCH (n:Universities)
MATCH (m:University)
MERGE (n)-[:NAME]->(m)

MATCH (n:Agent)
MATCH (m:B_Customer)
WHERE m.type = 'AGENT'
MERGE (m)-[:ISA]->(n)

MATCH (n:Amazon)
MATCH (m:B_Customer)
WHERE m.type = 'AMAZON'
MERGE (m)-[:ISA]->(n)


MATCH (n:School)
MATCH (m:B_Customer)
WHERE m.b_customer_name CONTAINS 'SCHOOL'
MERGE (m)-[:ISA]->(n)


MATCH (n:School)
MATCH (m:Customer)
WHERE m.customer_name CONTAINS 'SCHOOL'
MERGE (m)-[:ISA]->(n)

MATCH (n:Universities)
MATCH (m:B_Customer)
WHERE m.type = 'UNIVERSITIES'
MERGE (m)-[:ISA]->(n)

MATCH (n:Universities)
MATCH (m:Customer)
WHERE m.customer_name  CONTAINS 'UNIV'
MERGE (m)-[:ISA]->(n)


CREATE (waterstones:Waterstones {name:'WATERSTONES'})
MATCH (n:Waterstones)
MATCH (m:B_Customer)
WHERE m.type CONTAINS 'WATERSTONE'
MERGE (m)-[:ISA]->(n)


MATCH (n:Waterstones)
MATCH (m:Institution)
MERGE (m)-[r:TYPE]->(n)
SET r.detail='WATERSTONES'


LOAD CSV WITH HEADERS FROM "file:///world-universities1.csv" AS csvLine
WITH csvLine
LIMIT 1
RETURN csvLine;