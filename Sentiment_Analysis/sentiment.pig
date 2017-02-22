nas = load '/sentiment_output/*' as (tweet:chararray,count:int);
no = ORDER nas by count desc;
top = LIMIT no 200;
dump top;
