MATCH (article:Article)-[:PUBLISHED_IN]->(journal:Journal)
MATCH (journal)-[:PUBLISHED]->(year:Year)
MATCH (article)-[:PUBLISHED]->(year)
MATCH (affiliation:Affiliation)<-[:PUBLISHED_BY]-(article)
MATCH (customer:Customer)-[:BOUGHT]->(journal)
WHERE article.article_title CONTAINS 'ALZHEIM' AND year.year > 2015 AND journal.title CONTAINS 'Medicine' AND customer.customer_name CONTAINS 'ROMA'
RETURN journal,article,year,customer,affiliation;



MATCH (customer:Customer)-[:BOUGHT]->(journal)
WHERE  customer.customerid="0000000009"
RETURN customer;

LOAD CSV WITH HEADERS FROM "file:///Users/gf247/Documents/Dropbox/NEO4J-TEST/CUP-try/books_productV4.csv" AS csvLine
WITH csvLine
LIMIT 1
RETURN csvLine;


CREATE CONSTRAINT ON (book:Book) ASSERT book.title_code IS UNIQUE;

USING PERIODIC COMMIT 10000
LOAD CSV WITH HEADERS FROM "file:///Users/gf247/Documents/Dropbox/NEO4J-TEST/CUP-try/books_productV2.csv" AS csvLine
MERGE (book:Book {title_code:toInt(csvLine.TITLE_CODE)})


CREATE CONSTRAINT ON (isbn:Isbn) ASSERT isbn.isbn10 IS UNIQUE;

USING PERIODIC COMMIT 10000 // first check ISBN13 that has no blanks
LOAD CSV WITH HEADERS FROM "file:///Users/gf247/Documents/Dropbox/NEO4J-TEST/CUP-try/books_productV4.csv" AS csvLine
MERGE (isbn:Isbn {isbn10:csvLine.ISBN10, isbn13:csvLine.ISBN13, format:csvLine.FORMAT_LEGEND, title_full:UPPER(csvLine.TITLE_FULL)})


USING PERIODIC COMMIT 10000 // first check ISBN13 that has no blanks
LOAD CSV WITH HEADERS FROM "file:///Users/gf247/Documents/Dropbox/NEO4J-TEST/CUP-try/books_productV4.csv" AS csvLine
MATCH (book:Book {title_code:toInt(csvLine.TITLE_CODE)})
MATCH (isbn:Isbn {isbn10:csvLine.ISBN10, isbn13:csvLine.ISBN13, format:csvLine.FORMAT_LEGEND, title_full:csvLine.TITLE_FULL})
MERGE (book)<-[:HAS]-(isbn)

//careful with the delete!
USING PERIODIC COMMIT 10000 // first check ISBN13 that has no blanks
LOAD CSV WITH HEADERS FROM "file:///Users/gf247/Documents/Dropbox/NEO4J-TEST/CUP-try/books_productV4.csv" AS csvLine
MATCH (isbn:Isbn {isbn10:csvLine.ISBN10, isbn13:csvLine.ISBN13, format:csvLine.FORMAT_LEGEND, title_full:UPPER(csvLine.TITLE_FULL)})
DETACH DELETE isbn;

USING PERIODIC COMMIT 10000 // first check ISBN13 that has no blanks
LOAD CSV WITH HEADERS FROM "file:///Users/gf247/Documents/Dropbox/NEO4J-TEST/CUP-try/books_productV5.csv" AS csvLine
MERGE (isbn:Isbn {isbn10:csvLine.ISBN10, isbn13:csvLine.ISBN13, format:UPPER(csvLine.FORMAT_LEGEND), title_full:UPPER(csvLine.TITLE_FULL)})


USING PERIODIC COMMIT 10000 // first check ISBN13 that has no blanks
LOAD CSV WITH HEADERS FROM "file:///Users/gf247/Documents/Dropbox/NEO4J-TEST/CUP-try/books_productV5.csv" AS csvLine
MATCH (book:Book {title_code:toInt(csvLine.TITLE_CODE)})
MATCH (isbn:Isbn {isbn10:csvLine.ISBN10, isbn13:csvLine.ISBN13, format:UPPER(csvLine.FORMAT_LEGEND), title_full:UPPER(csvLine.TITLE_FULL)})
MERGE (book)<-[:HAS]-(isbn)

MATCH (book:Book)<--(isbn:Isbn)
WHERE isbn.title_full CONTAINS 'English'
RETURN isbn, book;
