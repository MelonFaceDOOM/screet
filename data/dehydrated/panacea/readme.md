data from here:
https://github.com/thepanacealab/covid19_twitter

The initial versions of this dataset [14,21] only included data collected from the publicly available Twitter Stream API with a collection process that gathered any available tweets within the daily restrictions from Twitter from January to 11 March, filtering them on the following 3 keywords: “coronavirus”, “2019nCoV”, ”corona virus”. We shifted our focus to exclusively collect COVID-19 tweets on 12 March 2020 with the following keywords: “COVD19”, “CoronavirusPandemic”, “COVID-19”, “2019nCoV”, “CoronaOutbreak”, “coronavirus”, “WuhanVirus”, thus the number of tweets gathered dramatically expanded the dataset. Please note that the Stream API only allows free access to a one percent sample of the daily stream of Twitter.

All the panacea dataset files after 2021-01-01 were partitioned into evenly distributed samples, which were then scraped.

The scraping process was halted on the 72nd sample since I realized the subsequent files contained some duplicate IDs and the process was inefficient.

samples/missing_ids.txt contains all the ids from the original files that were missing as of the 72nd file.