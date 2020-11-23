# flink
Name of the course followed - PluralSight#Getting Started with Stream Processing Using Apache Flink


In order to access the Twitter API - 
1. We need the (Access Token, Secret Key), and (API Key, Secret)
2. get these on apps.twitter.com
3. Need to be logged into your twitter account, and need a phone number associated with it.
4. create a new app, and get the required keys and save them.
5. you need a twitter dependency in pom.xml -
  <dependency>
    	<groupId>org.apache.flink</groupId>
    	<artifactId>flink-connector-twitter_2.10</artifactId>
    	<version>1.1.4-hadoop1</version>
    </dependency>
 6. The flink machine on which you run this code needs to reference this jar as well. Download the jar and add to lib folder in project. 
 7. Search for that artifactId and click on the maven repo link. Get the version for this artifact ID corresponding to the flink version - 1.1.4-hadoop1
 8. Download the JAR associated with it and copy it in the lib directory of your flink install folder. This will work because the lib folder is already on the classpath and flink will recognize the JARs that live on it.
 9. Need to restart flink to make sure that it picks up the new JAR file.
 10. Run commands - 
  10.1 flink-1.1.4/bin/stop-local.sh
  10.2 flink-1.1.4/bin/start-local.sh
 11. For production the steps are different. This setup works for local environment.

NOTES - https://onedrive.live.com/view.aspx?resid=C561B380B143D824%21182&id=documents
onenote:https://d.docs.live.net/c561b380b143d824/Documents/Apache%20Flink/
