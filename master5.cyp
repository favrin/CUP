CREATE CONSTRAINT ON (aa:Organisation) ASSERT aa.mvid IS UNIQUE

LOAD CSV WITH HEADERS FROM "file:///sub31.csv" AS line
MERGE (aa:Organisation {mvid: toInt(line.MASTERVISION_ID), organisation_name: UPPER(line.`INSTITUTION NAME`), organisation_country: UPPER(line.COUNTRY)});


CREATE CONSTRAINT ON (subid:Subscriber_Account) ASSERT subid.id IS UNIQUE

LOAD CSV WITH HEADERS FROM "file:///sub31.csv" AS line
MERGE (subid:Subscriber_Account {id: line.`JOURNALS SUBSCRIPTIONS:SHIPPING ACCOUNT`});


LOAD CSV WITH HEADERS FROM "file:///sub31.csv" AS line
MATCH (aa:Organisation {mvid: toInt(line.MASTERVISION_ID), organisation_name: UPPER(line.`INSTITUTION NAME`), organisation_country: UPPER(line.COUNTRY)})
MATCH (subid:Subscriber_Account {id: line.`JOURNALS SUBSCRIPTIONS:SHIPPING ACCOUNT`})
MERGE (aa)-[:ACCOUNT]->(subid);



CREATE CONSTRAINT ON (journal:Journal) ASSERT journal.mnemonic IS UNIQUE;

LOAD CSV WITH HEADERS FROM "file:///sub31.csv" AS line
MERGE (journal:Journal {mnemonic: line.`JOURNALS SUBSCRIPTIONS:JOURNAL MNEMONIC`, 
	journal_title:UPPER(line.`JOURNALS SUBSCRIPTIONS:JOURNAL NAME`)});



//LOAD CSV WITH HEADERS FROM "file:///sub.csv" AS line
//MATCH (journal:Journal {mnemonic: line.`JOURNALS SUBSCRIPTIONS:JOURNAL MNEMONIC`, 
//	journal_title:UPPER(line.`JOURNALS SUBSCRIPTIONS:JOURNAL NAME`)})
//SET journal.journal_subject = UPPER(line.`JOURNALS SUBSCRIPTIONS:SUBJECT`)
//SET journal.journal_format = UPPER(line.`JOURNALS SUBSCRIPTIONS:JOURNAL FORMAT`);



CREATE CONSTRAINT ON (subscription:Subscription) ASSERT subscription.seq IS UNIQUE;

LOAD CSV WITH HEADERS FROM "file:///sub31.csv" AS line
with distinct line, split(line.`JOURNALS SUBSCRIPTIONS:START DATE`, '/') as startdate
MERGE (subscription:Subscription {seq: toInt(line.SEQ), pound_val: toInt(line.SUB_POUND_VALUE), price_category:line.`JOURNALS SUBSCRIPTIONS:PRICE CATEGORY`})
SET subscription.year = toInt(startdate[2]);



LOAD CSV WITH HEADERS FROM "file:///sub31.csv" AS line
MATCH (subscription:Subscription {seq: toInt(line.SEQ)})
MATCH (journal:Journal {mnemonic: line.`JOURNALS SUBSCRIPTIONS:JOURNAL MNEMONIC`})
MATCH (subid:Subscriber_Account {id: line.`JOURNALS SUBSCRIPTIONS:SHIPPING ACCOUNT`})
MERGE (subid)-[:HAS_A]->(subscription)
MERGE (subscription)-[:TO_A]->(journal);



CREATE CONSTRAINT ON (subject:Subject) ASSERT subject.subject_code1 IS UNIQUE;

LOAD CSV WITH HEADERS FROM "file:///sub31.csv" AS line
MERGE ( subject:Subject { subject_code1: UPPER(line.`JOURNALS SUBSCRIPTIONS:SUBJECT`)});


LOAD CSV WITH HEADERS FROM "file:///sub31.csv" AS line
MATCH (subject:Subject { subject_code1: UPPER(line.`JOURNALS SUBSCRIPTIONS:SUBJECT`)})
MATCH (journal:Journal {mnemonic: line.`JOURNALS SUBSCRIPTIONS:JOURNAL MNEMONIC`})
MERGE (subject) <-[:SUBJECT]- (journal);

------US ----

LOAD CSV WITH HEADERS FROM "file:///sub-US1.csv" AS line
MERGE (aa:Organisation {mvid: toInt(line.MASTERVISION_ID), organisation_name: UPPER(line.`INSTITUTION NAME`), organisation_country: UPPER(line.COUNTRY)});

LOAD CSV WITH HEADERS FROM "file:///sub-US1.csv" AS line
//with split(line.DOC_DATE, '-') as date
//MERGE (isbn:Product {isbn10: line.ISBN10_CODE, cup_account_number: line.CUP_ACCOUNT_NUMBER, local_cust: toInt(line.LOCAL_CUST)})
return line
LIMIT 5;

LOAD CSV WITH HEADERS FROM "file:///sub-US1.csv" AS line
MERGE (subid:Subscriber_Account {id: line.`JOURNALS SUBSCRIPTIONS:SHIPPING ACCOUNT`});


LOAD CSV WITH HEADERS FROM "file:///sub-US1.csv" AS line
MATCH (aa:Organisation {mvid: toInt(line.MASTERVISION_ID), organisation_name: UPPER(line.`INSTITUTION NAME`), organisation_country: UPPER(line.COUNTRY)})
MATCH (subid:Subscriber_Account {id: line.`JOURNALS SUBSCRIPTIONS:SHIPPING ACCOUNT`})
MERGE (aa)-[:ACCOUNT]->(subid);

LOAD CSV WITH HEADERS FROM "file:///sub-US1.csv" AS line
MERGE (journal:Journal {mnemonic: line.`JOURNALS SUBSCRIPTIONS:JOURNAL MNEMONIC`, 
	journal_title:UPPER(line.`JOURNALS SUBSCRIPTIONS:JOURNAL NAME`)});

LOAD CSV WITH HEADERS FROM "file:///sub-US1.csv" AS line
with distinct line, split(line.`JOURNALS SUBSCRIPTIONS:START DATE`, '/') as startdate
MERGE (subscription:Subscription {seq: toInt(line.SEQ), pound_val: toInt(line.POUND_VAL), price_category:line.`JOURNALS SUBSCRIPTIONS:PRICE CATEGORY`})
SET subscription.year = toInt(startdate[2]);

LOAD CSV WITH HEADERS FROM "file:///sub-US1.csv" AS line
MATCH (subscription:Subscription {seq: toInt(line.SEQ)})
MATCH (journal:Journal {mnemonic: line.`JOURNALS SUBSCRIPTIONS:JOURNAL MNEMONIC`})
MATCH (subid:Subscriber_Account {id: line.`JOURNALS SUBSCRIPTIONS:SHIPPING ACCOUNT`})
MERGE (subid)-[:HAS_A]->(subscription)
MERGE (subscription)-[:TO_A]->(journal);


MATCH (aa:Organisation)-[]->(bb:Subscriber_Account) -[]->(cc:Subscription)-[]->(dd:Journal)-[]->(ee:Subject)
//MATCH (ff:Product)
WHERE aa.organisation_name CONTAINS 'HEIDELBERG'  
AND (ee.subject_code1 CONTAINS 'LIFE' 
OR ee.subject_code1 CONTAINS 'MEDICINE')
RETURN aa,bb,cc,dd,ee;



LOAD CSV WITH HEADERS FROM "file:///sub31.csv" AS line
MATCH (one:Subscription {seq: toInt(line.SEQ), pound_val: toInt(line.SUB_POUND_VALUE), price_category:line.`JOURNALS SUBSCRIPTIONS:PRICE CATEGORY`})
DETACH DELETE one; 

LOAD CSV WITH HEADERS FROM "file:///sub-UK1.csv" AS line
with distinct line, split(line.`JOURNALS SUBSCRIPTIONS:START DATE`, '/') as startdate
MERGE (subscription:Subscription {seq: toInt(line.SEQ), pound_val: toFloat(line.POUND_VAL), price_category:line.`JOURNALS SUBSCRIPTIONS:PRICE CATEGORY`})
SET subscription.year = toInt(startdate[2]);


LOAD CSV WITH HEADERS FROM "file:///sub-UK1.csv" AS line
MATCH (subscription:Subscription {seq: toInt(line.SEQ)})
MATCH (journal:Journal {mnemonic: line.`JOURNALS SUBSCRIPTIONS:JOURNAL MNEMONIC`})
MATCH (subid:Subscriber_Account {id: line.`JOURNALS SUBSCRIPTIONS:SHIPPING ACCOUNT`})
MERGE (subid)-[:HAS_A]->(subscription)
MERGE (subscription)-[:TO_A]->(journal);


LOAD CSV WITH HEADERS FROM "file:///sub-US1.csv" AS line
MATCH (aa:Organisation {mvid: toInt(line.MASTERVISION_ID), organisation_name: UPPER(line.`INSTITUTION NAME`), organisation_country: UPPER(line.COUNTRY)})
SET aa.organisation_city = UPPER(line.CITY)
SET aa.organisation_us_state = UPPER(line.`US STATE`);


LOAD CSV WITH HEADERS FROM "file:///sub-UK1.csv" AS line
MATCH (aa:Organisation {mvid: toInt(line.MASTERVISION_ID), organisation_name: UPPER(line.`INSTITUTION NAME`), organisation_country: UPPER(line.COUNTRY)})
SET aa.organisation_city = UPPER(line.CITY);


LOAD CSV WITH HEADERS FROM "file:///world_iso2.csv" AS line
return line
LIMIT 5;


CREATE CONSTRAINT ON (country:Country) ASSERT country.country_iso IS UNIQUE

LOAD CSV WITH HEADERS FROM "file:///world_iso2.csv" AS line
MERGE (country:Country {country_iso: UPPER(line.ISO)});



CREATE CONSTRAINT ON (us_state:US_State) ASSERT us_state.state_code IS UNIQUE


LOAD CSV WITH HEADERS FROM "file:///US-STATE.csv" AS line
MERGE (us_state:US_State {state_code: UPPER(line.US_STATE_CODE)});

LOAD CSV WITH HEADERS FROM "file:///US-STATE.csv" AS line
MATCH (us_state:US_State {state_code: UPPER(line.US_STATE_CODE)})
MATCH (usa:Country {country_iso: 'USA'})
MERGE (usa)-[:USSTATE]->(us_state);


CREATE CONSTRAINT ON (city:City) ASSERT city.city IS UNIQUE


LOAD CSV WITH HEADERS FROM "file:///city2.csv" AS line
MERGE (city:City {city: UPPER(line.CITY) });

LOAD CSV WITH HEADERS FROM "file:///city2-US.csv" AS line
MERGE (city:City {city: UPPER(line.CITY) });



CREATE CONSTRAINT ON (us_state:US_State) ASSERT us_state.state_name IS UNIQUE

LOAD CSV WITH HEADERS FROM "file:///US-STATE.csv" AS line
MERGE (us_state:US_State {state_code: UPPER(line.US_STATE_CODE), state_name: UPPER(line.Organisation_US_State)});

//SET us_state.state_name = UPPER(line.Organisation_US_State)
//DETACH DELETE us_state;


LOAD CSV WITH HEADERS FROM "file:///city2-US.csv" AS line
MATCH (city:City {city: UPPER(line.CITY) })
MATCH (us_state:US_State {state_name:UPPER(line.US_STATE)})
MERGE (city)<-[:USSTATECITY]-(us_state)


LOAD CSV WITH HEADERS FROM "file:///city2-US.csv" AS line
match (aa:US_State {state_name: 'OHIO'})
return aa;


LOAD CSV WITH HEADERS FROM "file:///city2-UK.csv" AS line
MATCH (city:City {city: UPPER(line.CITY) })
MATCH (uk:Country {country_iso: 'GBR'})
MERGE (city)<-[:COUNTRY]-(uk)


LOAD CSV WITH HEADERS FROM "file:///sub-UK1.csv" AS line
MATCH (aa:Organisation {mvid: toInt(line.MASTERVISION_ID)})
MATCH (bb:City {city: UPPER(line.CITY)})
MERGE (aa)<-[:ORGCITY]-(bb)

LOAD CSV WITH HEADERS FROM "file:///sub-US1.csv" AS line
MATCH (aa:Organisation {mvid: toInt(line.MASTERVISION_ID)})
MATCH (bb:City {city: UPPER(line.CITY)})
MERGE (aa)<-[:ORGCITY]-(bb)

CREATE CONSTRAINT ON (year:Year) ASSERT year.year IS UNIQUE

LOAD CSV WITH HEADERS FROM "file:///year.csv" AS line
MERGE (year:Year {year: toInt(line.YEAR)})

LOAD CSV WITH HEADERS FROM "file:///sub-UK1.csv" AS line
with distinct line, split(line.`JOURNALS SUBSCRIPTIONS:START DATE`, '/') as startdate
MATCH(year:Year {year: toInt(startdate[2])})
MATCH (subscription:Subscription {seq: toInt(line.SEQ)})
MERGE (year)<-[:MADEIN]-(subscription)


LOAD CSV WITH HEADERS FROM "file:///sub-US1.csv" AS line
with distinct line, split(line.`JOURNALS SUBSCRIPTIONS:START DATE`, '/') as startdate
MATCH(year:Year {year: toInt(startdate[2])})
MATCH (subscription:Subscription {seq: toInt(line.SEQ)})
MERGE (year)<-[:MADEIN]-(subscription)





return line
LIMIT 5;



LOAD CSV WITH HEADERS FROM "file:///sub-UK1.csv" AS line
with distinct line, split(line.`JOURNALS SUBSCRIPTIONS:START DATE`, '/') as startdate
MERGE (subscription:Subscription {seq: toInt(line.SEQ)})
SET subscription.pound_val =  toFloat(line.POUND_VAL);



LOAD CSV WITH HEADERS FROM "file:///sub-US1.csv" AS line
with distinct line, split(line.`JOURNALS SUBSCRIPTIONS:START DATE`, '/') as startdate
MERGE (subscription:Subscription {seq: toInt(line.SEQ)})
SET subscription.pound_val =  toFloat(line.POUND_VAL);




