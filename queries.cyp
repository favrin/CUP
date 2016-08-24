//MATCH (article:Article)-[:PUBLISHED_IN]->(journal:Journal)
//MATCH (journal)-[:PUBLISHED]->(year:Year)
//MATCH (article)-[:PUBLISHED]->(year)
//MATCH (affiliation:Affiliation)<-[:PUBLISHED_BY]-(article)
//MATCH (customer:Customer)-[:BOUGHT]->(journal)
//WHERE article.article_title CONTAINS 'ALZHEIM' AND year.year > 2015 AND journal.journal_title CONTAINS 'AGE' AND customer.customer_name CONTAINS 'ROMA'
//RETURN journal,article,year,customer,affiliation;


MATCH (subscription:Subscription) -[]->(journal:Journal)
WHERE journal.mnemonic = 'BBS' AND subscription.year = 2016
RETURN subscription, journal;


