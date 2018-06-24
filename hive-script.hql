CREATE EXTERNAL TABLE bigram (gram STRING, year INT, occurrences FLOAT, books FLOAT) ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t' LOCATION  '/user/hadoop/hive_gram/'; 

LOAD DATA INPATH '/user/hadoop/bigrams/googlebooks-eng-us-all-2gram-20120701-i?' OVERWRITE INTO TABLE bigram;	

INSERT OVERWRITE DIRECTORY '/user/hadoop/hiveresult/'
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
SELECT gram, total_occurences, total_books,  average_occurences, first_year, last_year, total_records
FROM (SELECT gram, SUM(bigram.occurrences) AS total_occurences, SUM (bigram.books) AS total_books,  SUM(bigram.occurrences)/SUM(bigram.books) AS average_occurences, MIN(bigram.year) AS first_year, MAX(bigram.year) AS last_year, COUNT(*) AS total_records
FROM bigram
GROUP BY gram) AS A
WHERE (first_year=1950) AND (total_records=60)
ORDER BY average_occurences DESC
LIMIT 10;

