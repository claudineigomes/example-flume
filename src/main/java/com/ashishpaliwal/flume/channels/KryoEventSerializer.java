package com.ashishpaliwal.flume.channels;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.StreamSerializer;
import org.apache.flume.Event;
import org.apache.flume.event.SimpleEvent;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

/**
 * Kryo based event serializer
 *
 * Used the code from Hazelcast book by pveentjer
 */
public class KryoEventSerializer implements StreamSerializer<Event> {

  private static final ThreadLocal<Kryo> kryoThreadLocal = new ThreadLocal<Kryo>() {
    @Override
    protected Kryo initialValue() {
      Kryo kryo = new Kryo();
      kryo.register(Event.class);
      return kryo;
    }
  };

  @Override
  public void write(ObjectDataOutput objectDataOutput, Event event) throws IOException {
    Kryo kryo = kryoThreadLocal.get();

    Output output = new Output((OutputStream) objectDataOutput);
    kryo.writeObject(output, event);
    output.flush();
  }

  @Override
  public Event read(ObjectDataInput objectDataInput) throws IOException {
    InputStream in = (InputStream) objectDataInput;
    Input input = new Input(in);
    Kryo kryo = kryoThreadLocal.get();
    return kryo.readObject(input, SimpleEvent.class);
  }

  @Override
  public int getTypeId() {
    return 2;
  }

  @Override
  public void destroy() {

  }
}
