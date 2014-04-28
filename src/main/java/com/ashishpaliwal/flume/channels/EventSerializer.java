package com.ashishpaliwal.flume.channels;

import com.hazelcast.nio.serialization.ByteArraySerializer;
import org.apache.flume.Event;
import org.apache.flume.event.SimpleEvent;

import java.io.IOException;

/**
 * Dumb serializer
 */
public class EventSerializer implements ByteArraySerializer<Event> {

  @Override
  public byte[] write(Event event) throws IOException {

    return event.getBody();
  }

  @Override
  public Event read(byte[] bytes) throws IOException {
    Event event = new SimpleEvent();
    event.setBody(bytes);
    return event;
  }

  @Override
  public int getTypeId() {
    return 101;
  }

  @Override
  public void destroy() {

  }
}
