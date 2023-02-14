package org.apache.rocketmq.spring.core;

import org.apache.rocketmq.client.apis.ClientConfiguration;
import org.apache.rocketmq.client.apis.ClientException;
import org.apache.rocketmq.client.apis.ClientServiceProvider;
import org.apache.rocketmq.client.apis.SessionCredentialsProvider;
import org.apache.rocketmq.client.apis.StaticSessionCredentialsProvider;
import org.apache.rocketmq.client.apis.message.Message;
import org.apache.rocketmq.client.apis.producer.Producer;
import org.apache.rocketmq.client.apis.producer.SendReceipt;
import org.junit.Test;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.concurrent.CompletableFuture;

public class RocketmqProxyTest {

  @Test
  public void testRocketmqProxyProducer() throws InterruptedException, IOException, ClientException {
    final ClientServiceProvider provider = ClientServiceProvider.loadService();

    // Credential provider is optional for client configuration.
    String accessKey = "yourAccessKey";
    String secretKey = "yourSecretKey";
    SessionCredentialsProvider sessionCredentialsProvider =
        new StaticSessionCredentialsProvider(accessKey, secretKey);

    String endpoints = "127.0.0.1:8081";
    ClientConfiguration clientConfiguration = ClientConfiguration.newBuilder()
        .setEndpoints(endpoints)
        .setCredentialProvider(sessionCredentialsProvider)
        .setRequestTimeout(Duration.ofSeconds(30))
        .build();
    String topic = "TopicTest";
    final Producer producer = provider.newProducerBuilder()
        .setClientConfiguration(clientConfiguration)
        // Set the topic name(s), which is optional. It makes producer could prefetch the topic route before
        // message publishing.
        .setTopics(topic)
        // May throw {@link ClientException} if the producer is not initialized.
        .build();
    // Define your message body.
    byte[] body = "This is a normal message for Apache RocketMQ".getBytes(StandardCharsets.UTF_8);
    String tag = "yourMessageTagA";


    final Message message = provider.newMessageBuilder()
        // Set topic for the current message.
        .setTopic(topic)
        // Message secondary classifier of message besides topic.
        .setTag(tag)
        // Key(s) of the message, another way to mark message besides message id.
        .setKeys("yourMessageKey-0e094a5f9d85")
        .setBody(body)
        .build();
    final CompletableFuture<SendReceipt> future = producer.sendAsync(message);
    future.whenComplete((sendReceipt, throwable) -> {
      if (null == throwable) {
        System.out.println("Send message successfully, messageId=" + sendReceipt.getMessageId());
      } else {
        System.out.println("Failed to send message");
      }
    });
    // Block to avoid exist of background threads.
    Thread.sleep(Long.MAX_VALUE);
    // Close the producer when you don't need it anymore.
    producer.close();
  }
}
