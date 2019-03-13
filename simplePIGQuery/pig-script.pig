bigrams = LOAD '${INPUT}' USING PigStorage('\t') AS (bigram:chararray, year:int, occurrences:float, books:float);
filbigrams = FILTER bigrams BY (occurrences >= 300) AND (books >= 12);
ggrams = GROUP filbigrams BY bigram;
result = FOREACH ggrams GENERATE group AS bigram, SUM(filbigrams.occurrences)/SUM(filbigrams.books) AS occperbook;
final_full = ORDER result BY occperbook DESC, bigram ASC;
final = LIMIT final_full 15;
STORE final INTO '${OUTPUT}';