create table raw_sentiment
(
tweet string
)
row format delimited 
;

load data local inpath '/home/gulshan/Desktop/hadoop/Sentiment_Analysis/Data/*' into table raw_sentiment;
create table sentiment
(
tweet string
)
row format delimited 
;
insert into TABLE sentiment SELECT get_json_object(tweet, "$.text") FROM raw_sentiment;

