package com.ashishpaliwal.flume.channels;

import com.hazelcast.config.Config;
import com.hazelcast.config.QueueConfig;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import org.apache.flume.*;
import org.apache.flume.conf.Configurables;
import org.apache.flume.event.EventBuilder;
import org.junit.*;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;


/**
 * Test Class for Replicating Channel
 */
public class TestReplicatingChannel {

  private Channel channel;

  private static HazelcastInstance hazelcastInstance;

  @BeforeClass
  public static void initHazelcastServer() {
    Config config = new Config();
    config.getGroupConfig().setName("flume");
    config.getGroupConfig().setName("dev").setPassword("dev-pass");
    QueueConfig queueConfig = new QueueConfig();
    queueConfig.setName(ChannelConstants.DEFAULT_QUEUE_NAME);
    config.addQueueConfig(queueConfig);
    hazelcastInstance = Hazelcast.newHazelcastInstance(config);
  }

  @AfterClass
  public static void destroyHazelcastServer() {
    hazelcastInstance.getLifecycleService().shutdown();
  }


  @Before
  public void setUp() {
    channel = new ReplicatingChannel();
  }

  @Test
  public void testPutTake() throws InterruptedException, EventDeliveryException {
    Event event = EventBuilder.withBody("test event".getBytes());
    Context context = new Context();

    Configurables.configure(channel, context);

    Transaction transaction = channel.getTransaction();
    Assert.assertNotNull(transaction);

    transaction.begin();
    channel.put(event);
    transaction.commit();
    transaction.close();

    transaction = channel.getTransaction();
    Assert.assertNotNull(transaction);

    transaction.begin();
    Event event2 = channel.take();
    Assert.assertEquals(event.getHeaders(), event2.getHeaders());
    Assert.assertTrue(Arrays.equals(event.getBody(), event2.getBody()));
    transaction.commit();
  }

  @Test(expected=ChannelException.class)
  public void testTransactionPutCapacityOverload() {
    Context context = new Context();
    Map<String, String> parms = new HashMap<String, String>();
    parms.put("capacity", "5");
    parms.put("transactionCapacity", "2");
    context.putAll(parms);
    Configurables.configure(channel,  context);

    Transaction transaction = channel.getTransaction();
    transaction.begin();
    channel.put(EventBuilder.withBody("test".getBytes()));
    channel.put(EventBuilder.withBody("test".getBytes()));
    // shouldn't be able to fit a third in the buffer
    channel.put(EventBuilder.withBody("test".getBytes()));
    Assert.fail();
  }

  @Test(expected=ChannelException.class)
  public void testCapacityOverload() {
    Context context = new Context();
    Map<String, String> parms = new HashMap<String, String>();
    parms.put("capacity", "5");
    parms.put("transactionCapacity", "3");
    context.putAll(parms);
    Configurables.configure(channel,  context);

    Transaction transaction = channel.getTransaction();
    transaction.begin();
    channel.put(EventBuilder.withBody("test".getBytes()));
    channel.put(EventBuilder.withBody("test".getBytes()));
    channel.put(EventBuilder.withBody("test".getBytes()));
    transaction.commit();
    transaction.close();

    transaction = channel.getTransaction();
    transaction.begin();
    channel.put(EventBuilder.withBody("test".getBytes()));
    channel.put(EventBuilder.withBody("test".getBytes()));
    channel.put(EventBuilder.withBody("test".getBytes()));
    // this should kill  it
    transaction.commit();
    Assert.fail();
  }

  @Test
  public void testCapacityBufferEmptyingAfterTakeCommit() {
    Context context = new Context();
    Map<String, String> parms = new HashMap<String, String>();
    parms.put("capacity", "3");
    parms.put("transactionCapacity", "3");
    context.putAll(parms);
    Configurables.configure(channel,  context);

    Transaction tx = channel.getTransaction();
    tx.begin();
    channel.put(EventBuilder.withBody("test".getBytes()));
    channel.put(EventBuilder.withBody("test".getBytes()));
    channel.put(EventBuilder.withBody("test".getBytes()));
    tx.commit();
    tx.close();

    tx = channel.getTransaction();
    tx.begin();
    channel.take();
    channel.take();
    tx.commit();
    tx.close();

    tx = channel.getTransaction();
    tx.begin();
    channel.put(EventBuilder.withBody("test".getBytes()));
    channel.put(EventBuilder.withBody("test".getBytes()));
    tx.commit();
    tx.close();
  }

  @Test
  public void testCapacityBufferEmptyingAfterRollback() {
    Context context = new Context();
    Map<String, String> parms = new HashMap<String, String>();
    parms.put("capacity", "3");
    parms.put("transactionCapacity", "3");
    context.putAll(parms);
    Configurables.configure(channel,  context);

    Transaction tx = channel.getTransaction();
    tx.begin();
    channel.put(EventBuilder.withBody("test".getBytes()));
    channel.put(EventBuilder.withBody("test".getBytes()));
    channel.put(EventBuilder.withBody("test".getBytes()));
    tx.rollback();
    tx.close();

    tx = channel.getTransaction();
    tx.begin();
    channel.put(EventBuilder.withBody("test".getBytes()));
    channel.put(EventBuilder.withBody("test".getBytes()));
    channel.put(EventBuilder.withBody("test".getBytes()));
    tx.commit();
    tx.close();
  }

  @Test(expected=ChannelException.class)
  public void testByteCapacityOverload() {
    Context context = new Context();
    Map<String, String> parms = new HashMap<String, String>();
    parms.put("byteCapacity", "2000");
    parms.put("byteCapacityBufferPercentage", "20");
    context.putAll(parms);
    Configurables.configure(channel,  context);

    byte[] eventBody = new byte[405];

    Transaction transaction = channel.getTransaction();
    transaction.begin();
    channel.put(EventBuilder.withBody(eventBody));
    channel.put(EventBuilder.withBody(eventBody));
    channel.put(EventBuilder.withBody(eventBody));
    transaction.commit();
    transaction.close();

    transaction = channel.getTransaction();
    transaction.begin();
    channel.put(EventBuilder.withBody(eventBody));
    channel.put(EventBuilder.withBody(eventBody));
    // this should kill  it
    transaction.commit();
    Assert.fail();

  }

  public void testByteCapacityBufferEmptyingAfterTakeCommit() {
    Context context = new Context();
    Map<String, String> parms = new HashMap<String, String>();
    parms.put("byteCapacity", "2000");
    parms.put("byteCapacityBufferPercentage", "20");
    context.putAll(parms);
    Configurables.configure(channel,  context);

    byte[] eventBody = new byte[405];

    Transaction tx = channel.getTransaction();
    tx.begin();
    channel.put(EventBuilder.withBody(eventBody));
    channel.put(EventBuilder.withBody(eventBody));
    channel.put(EventBuilder.withBody(eventBody));
    channel.put(EventBuilder.withBody(eventBody));
    try {
      channel.put(EventBuilder.withBody(eventBody));
      throw new RuntimeException("Put was able to overflow byte capacity.");
    } catch (ChannelException ce)
    {
      //Do nothing
    }

    tx.commit();
    tx.close();

    tx = channel.getTransaction();
    tx.begin();
    channel.take();
    channel.take();
    tx.commit();
    tx.close();

    tx = channel.getTransaction();
    tx.begin();
    channel.put(EventBuilder.withBody(eventBody));
    channel.put(EventBuilder.withBody(eventBody));
    try {
      channel.put(EventBuilder.withBody(eventBody));
      throw new RuntimeException("Put was able to overflow byte capacity.");
    } catch (ChannelException ce)
    {
      //Do nothing
    }
    tx.commit();
    tx.close();
  }

  @Test
  public void testByteCapacityBufferEmptyingAfterRollback() {
    Context context = new Context();
    Map<String, String> parms = new HashMap<String, String>();
    parms.put("byteCapacity", "2000");
    parms.put("byteCapacityBufferPercentage", "20");
    context.putAll(parms);
    Configurables.configure(channel,  context);

    byte[] eventBody = new byte[405];

    Transaction tx = channel.getTransaction();
    tx.begin();
    channel.put(EventBuilder.withBody(eventBody));
    channel.put(EventBuilder.withBody(eventBody));
    channel.put(EventBuilder.withBody(eventBody));
    tx.rollback();
    tx.close();

    tx = channel.getTransaction();
    tx.begin();
    channel.put(EventBuilder.withBody(eventBody));
    channel.put(EventBuilder.withBody(eventBody));
    channel.put(EventBuilder.withBody(eventBody));
    tx.commit();
    tx.close();
  }

  @Test
  public void testByteCapacityBufferChangeConfig() {
    Context context = new Context();
    Map<String, String> parms = new HashMap<String, String>();
    parms.put("byteCapacity", "2000");
    parms.put("byteCapacityBufferPercentage", "20");
    context.putAll(parms);
    Configurables.configure(channel,  context);

    byte[] eventBody = new byte[405];

    Transaction tx = channel.getTransaction();
    tx.begin();
    channel.put(EventBuilder.withBody(eventBody));
    tx.commit();
    tx.close();
    channel.stop();
    parms.put("byteCapacity", "1500");
    context.putAll(parms);
    Configurables.configure(channel,  context);
    channel.start();
    tx = channel.getTransaction();
    tx.begin();
    channel.put(EventBuilder.withBody(eventBody));
    try {
      channel.put(EventBuilder.withBody(eventBody));
      tx.commit();
      Assert.fail();
    } catch ( ChannelException e ) {
      //success
      tx.rollback();
    } finally {
      tx.close();
    }

    channel.stop();
    parms.put("byteCapacity", "250");
    parms.put("byteCapacityBufferPercentage", "20");
    context.putAll(parms);
    Configurables.configure(channel,  context);
    channel.start();
    tx = channel.getTransaction();
    tx.begin();
    channel.put(EventBuilder.withBody(eventBody));
    tx.commit();
    tx.close();
    channel.stop();

    parms.put("byteCapacity", "300");
    context.putAll(parms);
    Configurables.configure(channel,  context);
    channel.start();
    tx = channel.getTransaction();
    tx.begin();
    try {
      for(int i = 0; i < 2; i++) {
        channel.put(EventBuilder.withBody(eventBody));
      }
      tx.commit();
      Assert.fail();
    } catch ( ChannelException e ) {
      //success
      tx.rollback();
    } finally {
      tx.close();
    }

    channel.stop();
    parms.put("byteCapacity", "3300");
    context.putAll(parms);
    Configurables.configure(channel,  context);
    channel.start();
    tx = channel.getTransaction();
    tx.begin();

    try {
      for(int i = 0; i < 15; i++) {
        channel.put(EventBuilder.withBody(eventBody));
      }
      tx.commit();
      Assert.fail();
    } catch ( ChannelException e ) {
      //success
      tx.rollback();
    } finally {
      tx.close();
    }
    channel.stop();
    parms.put("byteCapacity", "4000");
    context.putAll(parms);
    Configurables.configure(channel,  context);
    channel.start();
    tx = channel.getTransaction();
    tx.begin();

    try {
      for(int i = 0; i < 25; i++) {
        channel.put(EventBuilder.withBody(eventBody));
      }
      tx.commit();
      Assert.fail();
    } catch ( ChannelException e ) {
      //success
      tx.rollback();
    } finally {
      tx.close();
    }
    channel.stop();
  }

  /*
   * This would cause a NPE without FLUME-1622.
   */
  @Test
  public void testNullEmptyEvent() {
    Context context = new Context();
    Map<String, String> parms = new HashMap<String, String>();
    parms.put("byteCapacity", "2000");
    parms.put("byteCapacityBufferPercentage", "20");
    context.putAll(parms);
    Configurables.configure(channel,  context);

    Transaction tx = channel.getTransaction();
    tx.begin();
    //This line would cause a NPE without FLUME-1622.
    channel.put(EventBuilder.withBody(null));
    tx.commit();
    tx.close();

    tx = channel.getTransaction();
    tx.begin();
    channel.put(EventBuilder.withBody(new byte[0]));
    tx.commit();
    tx.close();


  }
}
