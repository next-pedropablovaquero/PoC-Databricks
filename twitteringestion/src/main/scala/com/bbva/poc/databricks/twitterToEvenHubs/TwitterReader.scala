package com.bbva.poc.databricks.twitterToEvenHubs

import scala.collection.JavaConverters._
import com.microsoft.azure.eventhubs._
import java.util.concurrent._
import scala.collection.immutable._
import scala.concurrent.Future
import scala.concurrent.ExecutionContext.Implicits.global
import twitter4j._
import twitter4j.TwitterFactory
import twitter4j.Twitter
import twitter4j.conf.ConfigurationBuilder

object TwitterReader extends App {

  val namespaceName = "<EVENT HUBS NAMESPACE>"
  val eventHubName = "<EVENT HUB NAME>"
  val sasKeyName = "<POLICY NAME>"
  val sasKey = "<POLICY KEY>"
  val connStr = new ConnectionStringBuilder()
    .setNamespaceName(namespaceName)
    .setEventHubName(eventHubName)
    .setSasKeyName(sasKeyName)
    .setSasKey(sasKey)

  val pool = Executors.newScheduledThreadPool(1)
  val eventHubClient = EventHubClient.create(connStr.toString(), pool)

  def sleep(time: Long): Unit = Thread.sleep(time)

  def sendEvent(message: String, delay: Long) = {
    sleep(delay)
    val messageData = EventData.create(message.getBytes("UTF-8"))
    eventHubClient.get().send(messageData)
    System.out.println("Sent event: " + message + "\n")
  }

  // Add your own values to the list
  val testSource = List("Azure is the greatest!", "Azure isn't working :(", "Azure is okay.")

  // Specify 'test' if you prefer to not use Twitter API and loop through a list of values you define in `testSource`
  // Otherwise specify 'twitter'
  val dataSource = "twitter"

  if (dataSource == "twitter") {

    // Twitter configuration!
    // Replace values below with you

    val twitterConsumerKey = "<CONSUMER API KEY>"
    val twitterConsumerSecret = "<CONSUMER API SECRET>"
    val twitterOauthAccessToken = "<ACCESS TOKEN>"
    val twitterOauthTokenSecret = "<TOKEN SECRET>"

    val cb = new ConfigurationBuilder()
    cb.setDebugEnabled(true)
      .setOAuthConsumerKey(twitterConsumerKey)
      .setOAuthConsumerSecret(twitterConsumerSecret)
      .setOAuthAccessToken(twitterOauthAccessToken)
      .setOAuthAccessTokenSecret(twitterOauthTokenSecret)

    val twitterFactory = new TwitterFactory(cb.build())
    val twitter = twitterFactory.getInstance()

    // Getting tweets with keyword "Azure" and sending them to the Event Hub in realtime!
    val query = new Query(" #Azure ")
    query.setCount(100)
    query.lang("en")
    var finished = false
    while (!finished) {
      val result = twitter.search(query)
      val statuses = result.getTweets()
      var lowestStatusId = Long.MaxValue
      for (status <- statuses.asScala) {
        if(!status.isRetweet()){
          sendEvent(status.getText(), 5000)
        }
        lowestStatusId = Math.min(status.getId(), lowestStatusId)
      }
      query.setMaxId(lowestStatusId - 1)
    }

  } else if (dataSource == "test") {
    // Loop through the list of test input data
    while (true) {
      testSource.foreach {
        sendEvent(_,5000)
      }
    }

  } else {
    System.out.println("Unsupported Data Source. Set 'dataSource' to \"twitter\" or \"test\"")
  }

  // Closing connection to the Event Hub
  eventHubClient.get().close()

}
