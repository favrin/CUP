CREATE CONSTRAINT ON (customer:Customer) ASSERT customer.customer_key IS UNIQUE;

LOAD CSV WITH HEADERS FROM "file:///customers.csv" AS csvLine
MERGE (customer:Customer {customer_key: toInt(csvLine.CUSTOMER_KEY), customer_name: UPPER(csvLine.CUST_NAME), customer_country: UPPER(csvLine.COUNTRY)});

MATCH (n:Customer)
DETACH DELETE n;

LOAD CSV WITH HEADERS FROM "file:///wiki.csv" AS csvLine
MATCH(customer:Customer {customer_key: toInt(csvLine.CUSTOMER_KEY), customer_name: UPPER(csvLine.CUST_NAME), customer_country: UPPER(csvLine.COUNTRY)})
SET customer.customer_wikipedia=csvLine.WIKIPEDIA;


LOAD CSV WITH HEADERS FROM "file:///bodyid.csv" AS csvLine
MATCH(customer:Customer {customer_key: toInt(csvLine.CUSTOMER_KEY)})
SET customer.body_id=csvLine.BODY_ID;


LOAD CSV WITH HEADERS FROM "file:///bodyid.csv" AS csvLine
MATCH(customer:Customer {customer_key: toInt(csvLine.CUSTOMER_KEY)})
return customer;


LOAD CSV WITH HEADERS FROM "file:///cupacc.csv" AS csvLine
MATCH(customer:Customer {customer_key: toInt(csvLine.CUSTOMER_KEY)})
SET customer.cup_account_number=csvLine.CUP_ACCOUNT_NUMBER;


CREATE CONSTRAINT ON (isbn:Product) ASSERT isbn.isbn10 IS UNIQUE;

LOAD CSV WITH HEADERS FROM "file:///isbn.csv" AS line
MERGE (isbn:Product {isbn10: line.ISBN10, isbn13: line.ISBN13, title_code: line.TITLE_CODE});


LOAD CSV WITH HEADERS FROM "file:///transactions.csv" AS line
//MERGE (isbn:Product {isbn10: line.ISBN10_CODE, cup_account_number: line.CUP_ACCOUNT_NUMBER, local_cust: toInt(line.LOCAL_CUST)})
return line
LIMIT 5;

LOAD CSV WITH HEADERS FROM "file:///transaction15.csv" AS line
//with split(line.DOC_DATE, '-') as date
//MERGE (isbn:Product {isbn10: line.ISBN10_CODE, cup_account_number: line.CUP_ACCOUNT_NUMBER, local_cust: toInt(line.LOCAL_CUST)})
return line
LIMIT 5;


USING PERIODIC COMMIT 10000
LOAD CSV WITH HEADERS FROM "file:///transactions.csv" AS line
with distinct line, split(line.DOC_DATE, '-') as date
MATCH (customer:Customer {cup_account_number: line.CUP_ACCOUNT_NUMBER})
MATCH (isbn:Product {isbn10: line.ISBN10_CODE})
MERGE (customer)-[r:BOUGHT]->(isbn)
SET r.year = toInt(date[2])
SET r.month = date[1]
SET r.day = toInt(date[0])
SET r.local_cust = line.LOCAL_CUST

//MERGE (isbn:Product {isbn10: line.ISBN10_CODE, cup_account_number: line.CUP_ACCOUNT_NUMBER, local_cust: toInt(line.LOCAL_CUST)})



USING PERIODIC COMMIT 10000 
LOAD CSV WITH HEADERS FROM "file:///TRANSACTIONS-FEB-MAY-16.txt" AS csvLine
WITH distinct csvLine, SPLIT(csvLine.DOC_DATE, '/') AS date
MATCH (isbn:Isbn {isbn10:csvLine.ISBN10_CODE})
MATCH (b_customer:B_Customer {cup_account_n: toInt(csvLine.CUP_ACCOUNT_NUMBER) })
MERGE (isbn) -[r:B_BOUGHT]-> (b_customer)
SET r.year = toInt(date[2])
SET r.month = toInt(date[1])
SET r.day = toInt(date[0])

USING PERIODIC COMMIT 10000
LOAD CSV WITH HEADERS FROM "file:///transaction-short.csv" AS line
with distinct line, split(line.DOC_DATE, '-') as date
MATCH (customer:Customer {cup_account_number: line.CUP_ACCOUNT_NUMBER})
MATCH (isbn:Product {isbn10: line.ISBN10_CODE})
MERGE (customer)-[r:BOUGHT]->(isbn)
SET r.year = toInt(date[2])
SET r.month = date[1]
SET r.day = toInt(date[0])
SET r.local_cust = line.LOCAL_CUST;



CREATE (customer)-[:BOUGHT2]->(isbn);


CREATE CONSTRAINT ON ()-[r:BOUGHT]->() ASSERT isbn.isbn10 IS UNIQUE;




USING PERIODIC COMMIT 10000
LOAD CSV WITH HEADERS FROM "file:///transaction-short.csv" AS line
with distinct line, split(line.DOC_DATE, '-') as date
MATCH (customer:Customer {cup_account_number: line.CUP_ACCOUNT_NUMBER})
MATCH (isbn:Product {isbn10: line.ISBN10_CODE})
MERGE (customer)-[r:BOUGHT]->(isbn)
SET r.year = toInt(date[2])
SET r.month = date[1]
SET r.day = toInt(date[0])
SET r.local_cust = line.LOCAL_CUST;


CREATE CONSTRAINT ON (book_trans:Transaction) ASSERT book_trans.cup_account_number IS UNIQUE;

USING PERIODIC COMMIT 10000
LOAD CSV WITH HEADERS FROM "file:///transaction15.csv" AS line
with distinct line, split(line.DOC_DATE, '-') as date
MATCH (customer:Customer {cup_account_number: line.CUP_ACCOUNT_NUMBER})
MATCH (isbn:Product {isbn10: line.ISBN10_CODE})
MERGE (customer)-[r:BOUGHT]->(isbn)
SET r.year = toInt(date[2])
SET r.month = date[1]
SET r.day = toInt(date[0]);


MATCH (customer:Customer)-[]->(isbn:Isbn)
WHERE isbn.title_code = 445145
RETURN customer, isbn;



MATCH (customer:Customer)-[]->(isbn:Product)
WHERE customer.customer_name CONTAINS 'ASAN'
RETURN customer, isbn;