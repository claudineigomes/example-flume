package com.ashishpaliwal.flume.source;

import com.hazelcast.client.HazelcastClient;
import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.core.HazelcastInstance;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.EventDeliveryException;
import org.apache.flume.PollableSource;
import org.apache.flume.conf.Configurable;
import org.apache.flume.event.EventBuilder;
import org.apache.flume.source.AbstractSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;

/**
 * Flume source based on Hazelcast Queue.
 * Application can push data to hazelcast Queue and Source can read it from the Queue and push on.
 */
public class HazelcastQueueSource extends AbstractSource implements Configurable, PollableSource {

    public static final Logger LOGGER = LoggerFactory.getLogger(HazelcastQueueSource.class);

    // for simplicity use only string message to start with
    private BlockingQueue<String> distributedQueue;

    // Hazelcast client
//    private HazelcastInstance hazelcastClient;

    // Properties for Hazelcast
    private String queueName;
    private String serverIP;
    private String userName;
    private String userPwd;

    @Override
    public void configure(Context context) {
        // Get Hazelcast properties here
        queueName = context.getString("queueName");
        serverIP = context.getString("servers");
        userName = context.getString("user");
        userPwd = context.getString("password");
    }

    @Override
    public synchronized void start() {
        /*ClientConfig clientConfig = new ClientConfig();
        clientConfig.getGroupConfig().setName(userName).setPassword(userPwd);
        clientConfig.getNetworkConfig().addAddress(serverIP);
        hazelcastClient = HazelcastClient.newHazelcastClient(clientConfig);
        distributedQueue = hazelcastClient.getQueue(queueName);*/
    }



    @Override
    public synchronized void stop() {
//        hazelcastClient.shutdown();
        LOGGER.info("STOP");
    }

    @Override
    public Status process() throws EventDeliveryException {
        Status status = Status.READY;

        try {
            File file = new File("/home/claudinei/teste");
            FileInputStream fis = new FileInputStream(file);
            byte[] data = new byte[(int) file.length()];
            fis.read(data);
            fis.close();

            LOGGER.info("ESTOU AQUI");
            String msg = new String(data, "UTF-8");
            // not using charset here.
            if(msg == null) {
                return Status.BACKOFF;
            }
            Event event = EventBuilder.withBody(msg.getBytes());
            getChannelProcessor().processEvent(event);
        } catch (UnsupportedEncodingException e) {
            LOGGER.error("", e);
            status = Status.BACKOFF;
        } catch (FileNotFoundException e) {
            LOGGER.error("", e);
            status = Status.BACKOFF;
        } catch (IOException e) {
            LOGGER.error("", e);
            status = Status.BACKOFF;
        }

        return status;

    }
}
