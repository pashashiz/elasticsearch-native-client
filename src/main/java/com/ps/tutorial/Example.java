package com.ps.tutorial;

import java.io.IOException;
import java.net.InetAddress;
import java.util.Date;

import org.elasticsearch.action.admin.cluster.health.ClusterHealthResponse;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.discovery.MasterNotDiscoveredException;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;

public class Example {

    public static void main(String[] args) throws IOException {
        // Start a client
        Client client = TransportClient.builder().build()
                .addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName("localhost"), 9300));

        // Test 1: check the cluster status
        ClusterHealthResponse hr = null;
        try {
            hr = client.admin().cluster().prepareHealth().setWaitForGreenStatus()
                    .setTimeout(TimeValue.timeValueMillis(250)).execute().actionGet();
        } catch (MasterNotDiscoveredException e) {
            // No cluster status since we don't have a cluster
        }
        if (hr != null) {
            System.out.println("Data nodes found:" + hr.getNumberOfDataNodes());
            System.out.println("Timeout? :" + hr.isTimedOut());
            System.out.println("Status:" + hr.getStatus().name());
        }

        // Test 2: Add a document
        IndexResponse putResponse = client.prepareIndex("twitter", "tweet", "2")
                .setSource(jsonBuilder()
                        .startObject()
                            .field("user", "pavlo")
                            .field("postDate", new Date())
                            .field("message", "playing with Elasticsearch")
                        .endObject()
                )
                .get();
        System.out.println("Add document response:" + putResponse);

        // Test 3: Get a document
        GetResponse getResponse = client.prepareGet("twitter", "tweet", "2")
                .setOperationThreaded(false)
                .get();
        System.out.println("Get document response:" + getResponse.getSourceAsString());


    }

}
