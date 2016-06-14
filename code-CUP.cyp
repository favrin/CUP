LOAD CSV WITH HEADERS FROM "file:///Users/gf247/Documents/Neo4j/CUP-try/article_journal3.csv" AS csvLine
WITH csvLine
LIMIT 1
RETURN csvLine;



//--------
CREATE CONSTRAINT ON (journal:Journal) ASSERT journal.mnemonic IS UNIQUE;

//LOAD CSV WITH HEADERS FROM "file:///Users/gf247/Documents/Neo4j/CUP-try/journals3.csv" AS csvLine
//MERGE (journal:Journal {mnemonic: csvLine.mnemonic, title:csvLine.title, year:csvLine.year, articles:csvLine.narticles ,issues:csvLine.nissues, volumes:csvLine.nvolumes})
//RETURN journal;

USING PERIODIC COMMIT 500
LOAD CSV WITH HEADERS FROM "file:///Users/gf247/Documents/Neo4j/CUP-try/journals3.csv" AS csvLine
MERGE (journal:Journal {mnemonic: csvLine.mnemonic, title:csvLine.title})
//RETURN journal;

//customer 
CREATE CONSTRAINT ON (customer:Customer) ASSERT customer.customerid IS UNIQUE;

USING PERIODIC COMMIT 500
LOAD CSV WITH HEADERS FROM "file:///Users/gf247/Documents/Neo4j/CUP-try/customer-data-name-NB-3.csv" AS csvLine
MERGE (customer:Customer {customerid: toInt(csvLine.customerid), customer_name: UPPER(csvLine.customer_name), customer_brick: UPPER(csvLine.customer_brick)})


USING PERIODIC COMMIT 500
LOAD CSV WITH HEADERS FROM "file:///Users/gf247/Documents/Neo4j/CUP-try/mnemonic-userid.csv" AS csvLine
MATCH (customer:Customer {customerid: toInt(csvLine.customerid)})
MATCH (journal:Journal {mnemonic: csvLine.mnemonic})
MERGE (customer)-[:BOUGHT]->(journal)

MATCH (journal:Journal)<-[:BOUGHT]-(customer:Customer)
WHERE customer.customer_name CONTAINS 'ROMA TRE' AND journal.title CONTAINS 'Royal'
RETURN customer, journal;

USING PERIODIC COMMIT 500
LOAD CSV WITH HEADERS FROM "file:///Users/gf247/Documents/Neo4j/CUP-try/journals3.csv" AS csvLine
MATCH(journal:Journal {mnemonic: csvLine.mnemonic, title:csvLine.title})
MERGE (year:Year {year:toInt(csvLine.year)})
MERGE (journal)-[:PUBLISHED]->(year)



CREATE CONSTRAINT ON (article:Article) ASSERT article.articleid IS UNIQUE;
//do this second bit  above done ..

USING PERIODIC COMMIT 500
LOAD CSV WITH HEADERS FROM "file:///Users/gf247/Documents/Neo4j/CUP-try/article_journal4.csv" AS csvLine
MATCH (journal:Journal {mnemonic: csvLine.mnemonic})
MERGE (article:Article {articleid:toInt(csvLine.articleid), doi:csvLine.doi, article_title:UPPER(csvLine.article_title)})
MERGE (article)-[:PUBLISHED_IN]->(journal)

//query
MATCH (journal:Journal)<-[:PUBLISHED_IN]-(article:Article)
WHERE journal.mnemonic='BJN'
RETURN article, journal;
//correct title

USING PERIODIC COMMIT 500
LOAD CSV WITH HEADERS FROM "file:///Users/gf247/Documents/Neo4j/CUP-try/article_title.csv" AS csvLine
MATCH (article:Article {articleid:toInt(csvLine.articleid)})
SET article.article_title = UPPER(csvLine.article_title)

//query - search for journal title that published articles about alzheimer's 

MATCH (article:Article)-->(journal:Journal)
WHERE article.article_title CONTAINS 'ALZHEIM'
RETURN journal.title

//add article --> year 
// get the year data for journals (check first)

LOAD CSV WITH HEADERS FROM "file:///Users/gf247/Documents/Neo4j/CUP-try/article_date3.csv" AS csvLine
WITH SPLIT(csvLine.date,'-') AS date1
LIMIT 1
RETURN date1;

LOAD CSV WITH HEADERS FROM "file:///Users/gf247/Documents/Neo4j/CUP-try/article_date3.csv" AS csvLine
MATCH (article:Article {articleid:toInt(csvLine.articleid)})
MATCH (year:Year {year:toInt(csvLine.year)})
MERGE (article)-[:PUBLISHED]->(year)

//visualise the network of journal articles and dates for alzh.

MATCH (article:Article)-[:PUBLISHED_IN]->(journal:Journal)
MATCH (journal)-[:PUBLISHED]->(year:Year)
MATCH (article)-[:PUBLISHED]->(year)
MATCH (customer:Customer)-[:BOUGHT]->(journal)
WHERE article.article_title CONTAINS 'ALZHEIM' AND year.year > 2015 AND journal.title CONTAINS 'Medicine' AND customer.customer_name CONTAINS 'ROMA'
RETURN journal,article,year,customer;

//Add affiliation

USING PERIODIC COMMIT 500
// does this line make it much slower ? 
LOAD CSV WITH HEADERS FROM "file:///Users/gf247/Documents/Neo4j/CUP-try/article_Affiliation2.csv" AS csvLine
MATCH (article:Article {articleid:toInt(csvLine.articleid)})
MERGE (affiliation:Affiliation {affiliation: csvLine.affiliation}) // this should have been capitalised !!!!
// correct it using SET !!!!
MERGE (article)-[:PUBLISHED_BY]->(affiliation)

// new query
MATCH (article:Article)-[:PUBLISHED_IN]->(journal:Journal)
MATCH (journal)-[:PUBLISHED]->(year:Year)
MATCH (article)-[:PUBLISHED]->(year)
MATCH (affiliation:Affiliation)<-[:PUBLISHED_BY]-(article)
MATCH (customer:Customer)-[:BOUGHT]->(journal)
WHERE article.article_title CONTAINS 'ALZHEIM' AND year.year > 2015 AND journal.title CONTAINS 'Medicine' AND customer.customer_name CONTAINS 'ROMA'
RETURN journal,article,year,customer,affiliation;


MATCH (customer:Customer)
MATCH (affiliation:Affiliation)
WHERE customer.customer_name CONTAINS 'CALIFORNIA' AND affiliation.affiliation CONTAINS 'California'
RETURN customer, affiliation;

CREATE INDEX ON :Affiliation(affiliation)

// capitalise the affiliation 
LOAD CSV WITH HEADERS FROM "file:///Users/gf247/Documents/Dropbox/NEO4J-TEST/CUP-try/article_Affiliation2.csv" AS csvLine
MATCH (affiliation:Affiliation {affiliation: csvLine.affiliation}) 
SET affiliation.affiliation =  UPPER(csvLine.affiliation)

// remove Affiliation
MATCH (affiliation:Affiliation)
REMOVE affiliation.Affiliation

// add author 
LOAD CSV WITH HEADERS FROM "file:///Users/gf247/Documents/Dropbox/NEO4J-TEST/CUP-try/article_author5.csv" AS csvLine
MATCH (article:Article {articleid:toInt(csvLine.articleid)})
MATCH (journal:Journal {mnemonic: csvLine.mnemonic})
MATCH (affiliation:Affiliation {affiliation: UPPER(csvLine.affiliation)}) 
MERGE (author:Author {author_name: UPPER(csvLine.author_name)}) // this should have been capitalised !!!!
// correct it using SET !!!!
MERGE (author)-[:AUTHORED_BY]->(article)
MERGE (author)-[:AUTHORED_BY]->(journal)
MERGE (author)-[:AFFILIATED_TO]->(affiliation)


LOAD CSV WITH HEADERS FROM "file:///Users/gf247/Documents/Dropbox/NEO4J-TEST/CUP-try/article_author5.csv" AS csvLine
MATCH (affiliation:Affiliation {affiliation: UPPER(csvLine.affiliation)}) 
WHERE affiliation.affiliation CONTAINS 'DK-1790'
RETURN affiliation;


//correct affiliation
LOAD CSV WITH HEADERS FROM "file:///Users/gf247/Documents/Dropbox/NEO4J-TEST/CUP-try/article_Affiliation3.csv" AS csvLine
MATCH (affiliation:Affiliation {affiliation: UPPER(csvLine.affiliation)}) 
SET affiliation.affiliation =  UPPER(csvLine.affiliation2)

// new query 


MATCH (article:Article)<-[AUTHORED_BY]-(author:Author)
WHERE journal.title CONTAINS 'NETWORK SCIENCE' AND author.author_name CONTAINS 'VESPIGNANI'
return journal, author;


// new load 
CREATE INDEX ON :Author(author_name)
USING PERIODIC COMMIT 50000
LOAD CSV WITH HEADERS FROM "file:///Users/gf247/Documents/Dropbox/NEO4J-TEST/CUP-try/article_author5.csv" AS csvLine
MERGE (author:Author {author_name: UPPER(csvLine.author_name)})
//  update links to papers 
//delete nodes article
MATCH (article:Article)
MATCH (journal:Journal)
MATCH (author:Author)
DETACH DELETE article,journal,author;


//new

USING PERIODIC COMMIT 50000
LOAD CSV WITH HEADERS FROM "file:///Users/gf247/Documents/Dropbox/NEO4J-TEST/CUP-try/authordata3.csv" AS csvLine
MERGE (author:Author {author_name: UPPER(csvLine.author_name)})
MERGE (article:Article {articleid:toInt(csvLine.articleid), article_title:UPPER(csvLine.article_title)})
MERGE (journal:Journal {mnemonic: csvLine.mnemonic, journal_title:csvLine.journal_title})
MERGE (author)-[:AUTHORED_BY]->(article)
MERGE (article)-[:PUBLISHED_IN]->(journal)
MERGE (author)-[:AUTHORED_BY]->(journal)

