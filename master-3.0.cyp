CREATE CONSTRAINT ON (customer:Customer) ASSERT customer.customer_key IS UNIQUE;

LOAD CSV WITH HEADERS FROM "file:///customers.csv" AS csvLine
MERGE (customer:Customer {customer_key: toInt(csvLine.CUSTOMER_KEY), customer_name: UPPER(csvLine.CUST_NAME), customer_country: UPPER(csvLine.COUNTRY)});


LOAD CSV WITH HEADERS FROM "file:///wiki.csv" AS csvLine
MATCH(customer:Customer {customer_key: toInt(csvLine.CUSTOMER_KEY), customer_name: UPPER(csvLine.CUST_NAME), customer_country: UPPER(csvLine.COUNTRY)})
SET customer.customer_wikipedia=csvLine.WIKIPEDIA;


LOAD CSV WITH HEADERS FROM "file:///bodyid.csv" AS csvLine
MATCH(customer:Customer {customer_key: toInt(csvLine.CUSTOMER_KEY)})
SET customer.body_id=csvLine.BODY_ID;


LOAD CSV WITH HEADERS FROM "file:///cupacc.csv" AS csvLine
MATCH(customer:Customer {customer_key: toInt(csvLine.CUSTOMER_KEY)})
SET customer.cup_account_number=csvLine.CUP_ACCOUNT_NUMBER;


CREATE CONSTRAINT ON (isbn:Product) ASSERT isbn.isbn10 IS UNIQUE;

LOAD CSV WITH HEADERS FROM "file:///isbn.csv" AS line
MERGE (isbn:Product {isbn10: line.ISBN10, isbn13: line.ISBN13, title_code: line.TITLE_CODE});


CREATE CONSTRAINT ON (trans:Transaction) ASSERT trans.seq IS UNIQUE;

LOAD CSV WITH HEADERS FROM "file:///transaction1623.csv" AS line
//MERGE (isbn:Product {isbn10: line.ISBN10_CODE, cup_account_number: line.CUP_ACCOUNT_NUMBER, local_cust: toInt(line.LOCAL_CUST)})
return line
LIMIT 5;

MATCH (n:Customer)
DETACH DELETE n;



MATCH (n:Cup_account)
DETACH DELETE n;


LOAD CSV WITH HEADERS FROM "file:///transaction1623.csv" AS line
MERGE (trans:Transaction {seq: toInt(line.SEQ), doc_date: line.DOC_DATE, NET_VAL: toInt(line.NET_VAL)});



LOAD CSV WITH HEADERS FROM "file:///transaction16234.csv" AS line
MATCH (trans:Transaction {seq: toInt(line.SEQ)})
SET trans.currency=line.ORIG_CURRENCY;



LOAD CSV WITH HEADERS FROM "file:///transaction1623.csv" AS line
MATCH (customer:Customer {cup_account_number:line.CUP_ACCOUNT_NUMBER})
MATCH (isbn:Product {isbn10:line.ISBN10_CODE})
MATCH (transaction:Transaction {seq: toInt(line.SEQ)})
MERGE (customer)-[:BOUGHT]->(transaction)
MERGE (transaction) -[:DETAILS]-> (isbn);

USING PERIODIC COMMIT 10000
LOAD CSV WITH HEADERS FROM "file:///transaction163-short.csv" AS line
MATCH (customer:Customer {cup_account_number:line.CUP_ACCOUNT_NUMBER})
MATCH (trans:Transaction {seq: toInt(line.SEQ)})
MERGE (customer)-[:BOUGHT]->(trans);
