package com.ashishpaliwal.flume.examples;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.TimeUnit;

/**
 *
 */
public class Log4jAppenderExample {

  public static final Logger LOGGER = LoggerFactory.getLogger(Log4jAppenderExample.class);

  public void log(long count) {
    long msgCount = 0;

    while(msgCount++ < count) {
      System.out.println("dumping msg# "+msgCount);
      LOGGER.debug(String.format("Sending Msg#d to Flume", msgCount));
      try {
        Thread.currentThread().join(2000);
      } catch (InterruptedException e) {
        e.printStackTrace();
      }
    }
  }

  public static void main(String[] args) {
    Log4jAppenderExample log4jAppenderExample = new Log4jAppenderExample();
    log4jAppenderExample.log(Long.parseLong(args[0]));
  }

}
