package com.niks.config;

import com.niks.config.exception.ElasticSearchConnectionException;
import javax.annotation.PostConstruct;
import org.apache.http.HttpHost;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.client.RestHighLevelClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Configuration;

@Configuration
public class ElasticSearchConfig {

  @Value("${elasticsearch.host}")
  private String hostname;
  @Value("${elasticsearch.port}")
  private int port;

  private RestHighLevelClient restHighLevelClient;

  private static final Logger LOGGER = LoggerFactory.getLogger(ElasticSearchConfig.class);

  @PostConstruct
  private void init() {
    try {
      RestClientBuilder builder = RestClient.builder(new HttpHost(hostname, port, "http"));
      restHighLevelClient = new RestHighLevelClient(builder);
    } catch (Exception ex) {
      restHighLevelClient = null;
      LOGGER.error("Failed to connect elasticsearch. Check elastic search is up or not.", ex);
    }
  }

  public RestHighLevelClient getElasticSearchClient() {
    if(restHighLevelClient != null)
    return restHighLevelClient;
    else
      throw new ElasticSearchConnectionException("Elastic search client is not ready. Check elastic search is up or not.");
  }
}
