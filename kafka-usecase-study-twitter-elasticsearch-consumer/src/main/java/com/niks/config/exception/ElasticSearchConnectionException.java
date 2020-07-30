package com.niks.config.exception;

public class ElasticSearchConnectionException extends RuntimeException {

  public ElasticSearchConnectionException(final String errorMessage, final Throwable errorObject) {
    super(errorMessage, errorObject);
  }

  public ElasticSearchConnectionException(final String errorMessage) {
    super(errorMessage);
  }
}

