create table raw_tweets
(
tweet string
)
row format delimited 
;

load data local inpath '/home/gulshan/Downloads/FlumeData' into table raw_tweets;

insert into TABLE tweets SELECT get_json_object(tweet, "$.text") FROM raw_tweets;

