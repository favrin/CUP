CREATE CONSTRAINT ON (journal:Journal) ASSERT journal.mnemonic IS UNIQUE;
CREATE CONSTRAINT ON (author:Author) ASSERT author.author_name IS UNIQUE;
CREATE CONSTRAINT ON (article:Article) ASSERT article.articleid IS UNIQUE;

<<<<<<< HEAD
LOAD CSV WITH HEADERS FROM "file:///authordata3.csv" AS csvLine
=======
LOAD CSV WITH HEADERS FROM "file:///Users/gf247/Documents/Dropbox/NEO4J-TEST/CUP-try/authordata3.csv" AS csvLine
>>>>>>> 7033ef7aae60535871f7f5d60423e12fe51abce8
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
<<<<<<< HEAD
//CREATE CONSTRAINT ON (subscription:Affiliation) ASSERT affiliation.affiliation IS UNIQUE;


CREATE CONSTRAINT ON (book:Book) ASSERT book.title_code IS UNIQUE;

USING PERIODIC COMMIT 10000
LOAD CSV WITH HEADERS FROM "file:///Users/gf247/Documents/Dropbox/NEO4J-TEST/CUP-try/books_productV2.txt" AS csvLine
MERGE (book:Book {title_code:toInt(csvLine.TITLE_CODE)})


CREATE CONSTRAINT ON (isbn:Isbn) ASSERT isbn.isbn10 IS UNIQUE;

USING PERIODIC COMMIT 10000 // first check ISBN13 that has no blanks
LOAD CSV WITH HEADERS FROM "file:///Users/gf247/Documents/Dropbox/NEO4J-TEST/CUP-try/books_productV4.csv" AS csvLine
MERGE (isbn:Isbn {isbn10:csvLine.ISBN10, isbn13:csvLine.ISBN13, format:csvLine.FORMAT_LEGEND, title_full:csvLine.TITLE_FULL})


USING PERIODIC COMMIT 10000 // first check ISBN13 that has no blanks
LOAD CSV WITH HEADERS FROM "file:///Users/gf247/Documents/Dropbox/NEO4J-TEST/CUP-try/books_productV4.csv" AS csvLine
MATCH (book:Book {title_code:toInt(csvLine.TITLE_CODE)})
MATCH (isbn:Isbn {isbn10:csvLine.ISBN10, isbn13:csvLine.ISBN13, format:csvLine.FORMAT_LEGEND, title_full:csvLine.TITLE_FULL})
MERGE (book)<-[:HAS]-(isbn)

MATCH (book:Book)<--(isbn:Isbn)
WHERE isbn.title_full CONTAINS 'English'
RETURN isbn, book;
=======
CREATE CONSTRAINT ON (subscription:Affiliation) ASSERT affiliation.affiliation IS UNIQUE;


>>>>>>> 7033ef7aae60535871f7f5d60423e12fe51abce8

