
/**
 * Copyright 2019 Cathal Ryan
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 **/

import com.twitter.hbc.ClientBuilder;
import com.twitter.hbc.core.Constants;
import com.twitter.hbc.core.endpoint.StatusesSampleEndpoint;
import com.twitter.hbc.core.endpoint.StatusesFilterEndpoint;
import com.twitter.hbc.core.processor.StringDelimitedProcessor;
import com.twitter.hbc.httpclient.BasicClient;
import com.twitter.hbc.httpclient.auth.Authentication;
import com.twitter.hbc.httpclient.auth.OAuth1;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import java.util.List;
import com.google.common.collect.Lists;
import com.rabbitmqwrapper.MQwrapper.RabbitMQWrapper;

//import java.io.FileWriter;

public class TwitterStreamProducer {

	public static void run(String searchTerm, String consumerKey, String consumerSecret, String token, String secret) throws Exception {
    //  Create an appropriately sized blocking queue
    BlockingQueue<String> queue = new LinkedBlockingQueue<String>(10000);

	// Add words, hashtags to track. Defined in cmd argument
    StatusesFilterEndpoint endpoint = new StatusesFilterEndpoint();
	List<String> terms = Lists.newArrayList(searchTerm);
	endpoint.trackTerms(terms);
    endpoint.stallWarnings(false);

    Authentication auth = new OAuth1(consumerKey, consumerSecret, token, secret);
    //Authentication auth = new com.twitter.hbc.httpclient.auth.BasicAuth(username, password);

    // Create a new BasicClient. By default gzip is enabled.
    BasicClient client = new ClientBuilder()
            .name("TwitterStreamProducer")
            .hosts(Constants.STREAM_HOST)
            .endpoint(endpoint)
            .authentication(auth)
            .processor(new StringDelimitedProcessor(queue))
            .build();

    // Establish a connection
    client.connect();
	
	// Create wrapper object to be used to send to the queue
	RabbitMQWrapper rmq = RabbitMQWrapper.getrmqw("IncomingTweetQueue");
	//For creating test files
	//FileWriter fileWriter = new FileWriter("test.json");
	//fileWriter.write("{\n");
	try {
    for (int msgRead = 0; msgRead < 10000; msgRead++) {
      if (client.isDone()) {
    	
        System.out.println("Client connection closed unexpectedly: " + client.getExitEvent().getMessage());
        break;
      }
      String msg = queue.poll(5, TimeUnit.SECONDS);
      if (msg == null) {
        System.out.println("Waiting...");
		System.out.println(" ");
		
      } else {
    	//fileWriter.write(msg + ",\n");
		rmq.writeToQueue(msg);
      }
    }
	}
	finally {
		//fileWriter.write("}");
		//fileWriter.close();
		client.stop();
	}
    

  }

	public static void main(String[] args) {
		try {
			TwitterStreamProducer.run(args[0], args[1], args[2], args[3], args[4]);
		} catch (InterruptedException e) {
			System.out.println(e);
		} catch (Exception e) {
			System.out.println(e);
		}
	}
}