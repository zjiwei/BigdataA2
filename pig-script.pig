--pig script for bigram

ibigram = LOAD '/user/hadoop/bigrams/googlebooks-eng-us-all-2gram-20120701-i?' USING PigStorage('\t') AS ( gram:chararray, year:int, occurrences:float, books:float);

gramgroup = GROUP ibigram BY gram;

stat= FOREACH gramgroup GENERATE group, SUM(ibigram.occurrences) AS total_occurences, SUM (ibigram.books) AS total_books,  SUM(ibigram.occurrences)/SUM(ibigram.books) AS average_occurences, MIN(ibigram.year) AS first_year, MAX(ibigram.year) AS last_year, COUNT(ibigram) AS total_records;

stat_filtered = FILTER stat BY (first_year == 1950) AND (last_year == 2009) AND (total_records == 60);

stat_f_o= ORDER stat_filtered BY average_occurences DESC;

results = LIMIT stat_f_o 10;

STORE results INTO 'pig-results';



