package com.ashishpaliwal.flume.source;

import com.hazelcast.client.HazelcastClient;
import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.core.HazelcastInstance;

import java.util.concurrent.BlockingQueue;

/**
 * Hazelcast Queue Client to push messages to the Source
 */
public class HazelcastQueueClient {

    private BlockingQueue<String> distributedQueue;

    private HazelcastInstance hazelcastClient;

    /**
     * Initialize
     */
    public void init(String queueName) {
        ClientConfig clientConfig = new ClientConfig();
        clientConfig.getGroupConfig().setName("dev").setPassword("dev-pass");
        clientConfig.getNetworkConfig().addAddress("127.0.01");
        hazelcastClient = HazelcastClient.newHazelcastClient(clientConfig);
        distributedQueue = hazelcastClient.getQueue(queueName);
    }

    /**
     * Sends the message to Distributed Queue
     *
     * @param numberOfTimes number of times msg has to be sent
     */
    public void sendTextMessages(long numberOfTimes) {
        for (int i = 0; i < numberOfTimes; i++) {
            try {
                distributedQueue.put("Message# "+i);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }

    public static void main(String[] args) {
        HazelcastQueueClient hazelcastQueueClient = new HazelcastQueueClient();
        hazelcastQueueClient.init(args[0]);
        hazelcastQueueClient.sendTextMessages(Integer.parseInt(args[1]));
    }

}
