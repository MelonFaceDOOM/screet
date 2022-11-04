This code allows you to hydrate large sets of Tweet IDs. Requries Twitter dev account and associated bearer token
1) support for ID source that is split over many files by splitting them into evenly distribured samples
2) hydrate using requests. No python twitter libraries required.
3) saves raw json response from twitter
4) includes ability to parse these responses and then import them into a database

relevant links:
https://developer.twitter.com/apitools/api?endpoint=%2F2%2Ftweets%2F%7Bid%7D&method=get
https://oauth-playground.glitch.me
https://oauth-playground.glitch.me/?id=findTweetsById&params=%28%27expansionCBgeo.place_id%27%7Etweet-B*conversation_*Ageo%2Cin_reply_to_user_*entitieF0texEreferenced_tweets%27%7Euser-*0verifieDAlocation%27%7Eplace-country%27%7EidC1344872675286499328%27%29*iD-.fieldC0public_metricFAcreated_aEBauthor_*Cs%21%27Dd%2CEt%2CFs%2C%01FEDCBA0-*_