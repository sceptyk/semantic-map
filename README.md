# Tweetcity
Semantic map of Dublin based on Twitter activity. Using public Twitter Stream we obtain location, tweet message and time values which then are processed and stored.

### Licence
GNU GPLv3

### Usage
###### Code
To collect your own data you will need change area filter in /collector/twitter_stream_collector.py in the last line. It could be skipped if no area filter is needed to be applied. The next step is configuring /collector/mysql_connect.py file so it matches you database credentials. And that's it. You can run all components from app.py file or each one seperately.
* main_collector.py - Script to collect data from Twitter Stream
* main_processor.py - Script to process collected data, calculate word clouds and bind keywords with tweets.
* main_generator.py - Script responsible for running HTTP server, handling API calls and serving static application.

###### Website
You can play with the website and discover your city. The website provides heat map of activity based on the most recent Twitter users' activity as well as the whole history of tweets that we have. To switch between precision click on the icon of graph on the left of searching bar in the right top corner. In the right bottom corner you will notice the graph representing popularity during the day. In the left bottom corner the word-cloud is displayed. As you zoom in word-cloud will be updated based on the center of the map. You can also click on the map to see the cloud for specific area. Word clouds are associated with layer of precision which changes automatically as you zoom in. Additionally you can filter data by keyword, day of the week, day time or date. To expand menu click on filter icon in the very right top corner.

### Application
The website can find its application in different scenarios. The most promising one would marketing research. The geospatial analisys could provide 

### Future work
* Implementation of machine learning clustering algorithms to find places similar in activity.
* Request limit based on IP of caller.
* Dynamic url in web-app.
* Server framework upgrade, static content optimization.
* Migrating to geospatial database.

### Dataset provider
Geotagged twitter data provided by Dr. Ate Poorthuis and Dr. Matthew Zook via the DOLLY project at the University of Kentucky (http://www.floatingsheep.org/p/dolly.html)

### Libraries used:
Application-side:
* jQuery 3.0 (http://jquery.com/)
* jQuery UI (http://jqueryui.com/)
* Bootstrap 3 (http://getbootstrap.com/)
* Wordcloud2.js (https://github.com/timdream/wordcloud2.js)
* Charts.js (http://www.chartjs.org/)
Server-side:
* MySQL-python 1.2.5 (https://github.com/farcepest/MySQLdb1)
* tweepy 3.5.0 (http://tweepy.readthedocs.io/en/v3.5.0/)
* cherrypy 3.6.0 (http://www.cherrypy.org/)
* simplejson 3.8.2 (https://github.com/simplejson/simplejson)

### Authors
Filip Nowakowski
Vsevolods Caka
@ UCD Computer Science