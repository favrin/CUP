CREATE CONSTRAINT ON (customer:Customer) ASSERT customer.customer_key IS UNIQUE;

//LOAD CSV WITH HEADERS FROM "file:///customers.csv" AS line
//MERGE (customer:Customer {customer_key: toInt(line.CUSTOMER_KEY), customer_name: UPPER(line.CUST_NAME), customer_country: UPPER(line.COUNTRY)});

LOAD CSV WITH HEADERS FROM "file:///customers.csv" AS line
MERGE (customer:Customer {customer_key: toInt(line.CUSTOMER_KEY), customer_name: UPPER(line.CUST_NAME), customer_country: UPPER(line.COUNTRY)});



CREATE CONSTRAINT ON (cup_account:Cup_account) ASSERT cup_account.cup_account_number IS UNIQUE;

LOAD CSV WITH HEADERS FROM "file:///cupacc.csv" AS line
MERGE (cup_account:Cup_account {cup_account_number: toInt(line.CUP_ACCOUNT_NUMBER)});


CREATE CONSTRAINT ON (subscriber:Subscriber) ASSERT subscriber.subscriber_id IS UNIQUE;

LOAD CSV WITH HEADERS FROM "file:///subscrib.csv" AS line
MERGE (subscriber:Subscriber {subscriber_id: toInt(line.SUBSCRIBER_ID)});


CREATE CONSTRAINT ON (journal:Journal) ASSERT journal.mnemonic IS UNIQUE;

LOAD CSV WITH HEADERS FROM "file:///journals.csv" AS line
MERGE (journal:Journal {mnemonic: line.mnemonic, journal_title:UPPER(line.journal_title)});


CREATE CONSTRAINT ON (isbn:Product) ASSERT isbn.isbn10 IS UNIQUE;

LOAD CSV WITH HEADERS FROM "file:///book_products.csv" AS line
MERGE (isbn:Product {isbn10: line.ISBN10, isbn13: line.ISBN13, title_full:UPPER(line.TITLE_FULL), title_code: line.TITLE_CODE});


LOAD CSV WITH HEADERS FROM "file:///book_products.csv" AS line
MATCH (isbn:Product {isbn10: line.ISBN10})
SET isbn.product_group_code= line.PRODUCT_GROUP_CODE
SET isbn.publication_date=line.PUBLICATION_DATE;


LOAD CSV WITH HEADERS FROM "file:///book_products.csv" AS line
MATCH (isbn:Product {isbn10: line.ISBN10})
SET isbn.series_code= line.SERIES_CODE
SET isbn.format_code=line.FORMAT_CODE;





CREATE CONSTRAINT ON (transaction:Transaction) ASSERT transaction.seq IS UNIQUE;

//USING PERIODIC COMMIT 10000
LOAD CSV WITH HEADERS FROM "file:///transactions16.csv" AS line
MERGE (transaction:Transaction {seq: toInt(line.SEQ), doc_date: line.DOC_DATE, net_val: toInt(line.NET_VAL)});


LOAD CSV WITH HEADERS FROM "file:///transactions15.csv" AS line
MERGE (transaction:Transaction {seq: toInt(line.SEQ), doc_date: line.DOC_DATE, net_val: toInt(line.NET_VAL)});


MATCH (n:Product)
DETACH DELETE n;

//LOAD CSV WITH HEADERS FROM "file:///transaction1623.csv" AS line
//MATCH (trans:Transaction {seq: toInt(line.SEQ)})
//SET trans.currency=line.ORIG_CURRENCY;



//think about the transactions - node or relationship


LOAD CSV WITH HEADERS FROM "file:///transactions16.csv" AS line
MATCH (cup_account:Cup_account {cup_account_number:toInt(line.CUP_ACCOUNT_NUMBER)})
MATCH (isbn:Product {isbn10:line.ISBN10_CODE})
MATCH (transaction:Transaction {seq: toInt(line.SEQ)})
MERGE (cup_account)-[:BOUGHT]->(transaction)
MERGE (transaction) -[:DETAILS]-> (isbn);



LOAD CSV WITH HEADERS FROM "file:///transactions15.csv" AS line
MATCH (cup_account:Cup_account {cup_account_number:toInt(line.CUP_ACCOUNT_NUMBER)})
MATCH (isbn:Product {isbn10:line.ISBN10_CODE})
MATCH (transaction:Transaction {seq: toInt(line.SEQ)})
MERGE (cup_account)-[:BOUGHT]->(transaction)
MERGE (transaction) -[:DETAILS]-> (isbn);



LOAD CSV WITH HEADERS FROM "file:///cupacc.csv" AS line
MATCH (cup_account:Cup_account {cup_account_number: toInt(line.CUP_ACCOUNT_NUMBER)})
MATCH (customer:Customer {customer_key: toInt(line.CUSTOMER_KEY)})
MERGE (customer)-[:HAS]->(cup_account);




LOAD CSV WITH HEADERS FROM "file:///subscrib.csv" AS line
MATCH (subscriber:Subscriber {subscriber_id: toInt(line.SUBSCRIBER_ID)})
MATCH (customer:Customer {customer_key: toInt(line.CUSTOMER_KEY)})
MERGE (customer)-[:IS]->(subscriber);




LOAD CSV WITH HEADERS FROM "file:///authordata3.csv" AS line
//with split(line.DOC_DATE, '-') as date
//MERGE (isbn:Product {isbn10: line.ISBN10_CODE, cup_account_number: line.CUP_ACCOUNT_NUMBER, local_cust: toInt(line.LOCAL_CUST)})
return line
LIMIT 5;

MATCH (aa:Customer)-[:HAS]->(bb:Cup_account)
MATCH (bb)-[:BOUGHT]->(dd:Transaction)
MATCH (dd)-[:DETAILS]->(cc:Product)
WHERE aa.customer_name CONTAINS 'WISCONSIN' AND cc.title_full CONTAINS 'RUSSI'
RETURN aa,bb,dd,cc;




LOAD CSV WITH HEADERS FROM "file:///cup003b_subs_data.txt" AS line FIELDTERMINATOR '|'
//with split(line.DOC_DATE, '-') as date
//MERGE (isbn:Product {isbn10: line.ISBN10_CODE, cup_account_number: line.CUP_ACCOUNT_NUMBER, local_cust: toInt(line.LOCAL_CUST)})
return line
LIMIT 5;

MATCH (aa:Customer)-[:IS]->(bb:Subscriber)
WHERE aa.customer_name CONTAINS 'DELHI' AND aa.customer_name CONTAINS 'UNI'
RETURN aa,bb;


USING PERIODIC COMMIT 10000
LOAD CSV WITH HEADERS FROM "file:///cup003b_subs_data.txt" AS line FIELDTERMINATOR '|'
WITH DISTINCT line, split(line.SUB_START_DATE, '-') AS date
MATCH (subscriber:Subscriber {subscriber_id:line.SUB_END_USER_CODE})
MATCH (journal:Journal {mnemonic:line.JNL_MNE})
where date[0] = '2016'
//MERGE (subscriber)-[:TO]->(journal)
RETURN line
LIMIT 1; 




MATCH (aa:Customer)-[:HAS]->(bb:Cup_account)
WHERE bb.cup_account_number CONTAINS '-'
RETURN aa,bb;



MATCH (aa:Customer)-[:HAS]->(bb:Cup_account) -[:BOUGHT]->(dd:Transaction)-[:DETAILS]->(cc:Product)
MATCH (ff:Product)
WHERE ff <> cc AND  aa.customer_name CONTAINS 'WISCONSIN' AND aa.customer_name CONTAINS 'COLLEG' AND 
RETURN aa,cc,ff;



MATCH (aa:Customer)-[:HAS]->(bb:Cup_account) -[:BOUGHT]->(dd:Transaction)-[:DETAILS]->(cc:Product)
//MATCH (ff:Product)
WHERE aa.customer_name CONTAINS 'WISCONSIN' AND aa.customer_name CONTAINS 'COLLEG' 
AND cc.product_group_code CONTAINS 'A' 
AND cc.title_full CONTAINS 'RUSSIA'
RETURN aa,cc;

MATCH (ff:Product) <-[]-()
WHERE ff.product_group_code CONTAINS 'A' AND ff.title_full CONTAINS 'RUSSIA'
RETURN ff;



MATCH (gg:Cup_account)-[BOUGHT]->(jj:Transaction)-[:DETAILS]->(ff:Product)
where 



//MATCH (aa:Customer)-[:HAS]->(bb:Cup_account) -[:BOUGHT]->(dd:Transaction)-[:DETAILS]->(cc:Product)
MATCH (eaa:Customer)-[:HAS]->(ebb:Cup_account) -[:BOUGHT]->(edd:Transaction)-[:DETAILS]->(ecc:Product)
WHERE NOT(eaa.customer_name CONTAINS 'WISCONSIN' AND eaa.customer_name CONTAINS 'COLLEG') 
AND ecc.product_group_code CONTAINS 'A' 
AND ecc.title_full CONTAINS 'RUSSIA' 
RETURN eaa,ecc;