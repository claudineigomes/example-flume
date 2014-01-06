package com.ashishpaliwal.flume;

import com.google.common.base.Charsets;
import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.curator.utils.EnsurePath;

import java.io.BufferedReader;
import java.io.FileReader;

/**
 *
 */
public class ZookeeperUploader {

  public static void main(String[] args) throws Exception {
    EnsurePath ensurePath = new EnsurePath(args[0]);
    RetryPolicy retryPolicy = new ExponentialBackoffRetry(1000, 3);
    CuratorFramework client = CuratorFrameworkFactory.newClient(args[1], retryPolicy);
    client.start();
    ensurePath.ensure(client.getZookeeperClient());
    BufferedReader reader = new BufferedReader(new FileReader(args[2]));
    StringBuilder stringBuilder = new StringBuilder();
    String line = null;
    while ((line = reader.readLine()) != null) {
      stringBuilder.append(line).append("\n");
    }
    client.setData().forPath(args[0], stringBuilder.toString().getBytes(Charsets.UTF_8));
  }

}
