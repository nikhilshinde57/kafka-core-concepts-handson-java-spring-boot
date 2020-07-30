package com.niks.service;

import com.google.gson.JsonParser;
import com.niks.config.ElasticSearchConfig;
import java.io.IOException;
import java.util.List;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.common.xcontent.XContentType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

@Service
public class ElasticSearchService {

  @Autowired
  ElasticSearchConfig elasticSearchConfig;

  private static final Logger LOGGER = LoggerFactory.getLogger(ElasticSearchService.class);
  private static JsonParser jsonParser = new JsonParser();

  public boolean saveTweets(List<String> listOfTweets) throws IOException {

    BulkRequest bulkRequest = prepareBulkRequest(listOfTweets);
    LOGGER.info(String.format("Number valid tweets in bulk requests are: %d", bulkRequest.numberOfActions()));
    if(bulkRequest.numberOfActions()>0){
      BulkResponse bulkItemResponses = elasticSearchConfig.getElasticSearchClient().bulk(bulkRequest, RequestOptions.DEFAULT);
      try {
        Thread.sleep(1000);
        return bulkItemResponses.hasFailures();
      } catch (InterruptedException e) {
        e.printStackTrace();
        return bulkItemResponses.hasFailures();
      }
    }
    return false;
  }

  private BulkRequest prepareBulkRequest(List<String> listOfTweets){
    BulkRequest bulkRequest = new BulkRequest();
    listOfTweets.forEach(tweet->{
      try {
        String id = extractIdFromTweet(tweet);

        // where we insert data into ElasticSearch
        IndexRequest indexRequest = new IndexRequest(
            "twitter",
            "tweets",
            id // this is to make our consumer idempotent
        ).source(tweet, XContentType.JSON);

        bulkRequest.add(indexRequest); // we add to our bulk request (takes no time)
      } catch (Exception e){
        LOGGER.warn(String.format("Skipping bad data: %s", tweet));
      }
    });

    return bulkRequest;
  }

  private static String extractIdFromTweet(String tweetJson){
    // gson library
    return jsonParser.parse(tweetJson)
        .getAsJsonObject()
        .get("id_str")
        .getAsString();
  }
}
