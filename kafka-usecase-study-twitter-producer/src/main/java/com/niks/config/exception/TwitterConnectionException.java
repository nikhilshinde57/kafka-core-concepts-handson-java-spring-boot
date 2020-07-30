package com.niks.config.exception;

public class TwitterConnectionException extends RuntimeException {

  public TwitterConnectionException(final String errorMessage, final Throwable errorObject) {
    super(errorMessage, errorObject);
  }

  public TwitterConnectionException(final String errorMessage) {
    super(errorMessage);
  }
}
