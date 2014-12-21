package edu.sjsu.cmpe.cache.client;

import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;

import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.ListMultimap;
import com.mashape.unirest.http.Headers;
import com.mashape.unirest.http.HttpResponse;
import com.mashape.unirest.http.JsonNode;
import com.mashape.unirest.http.Unirest;
import com.mashape.unirest.http.async.Callback;
import com.mashape.unirest.http.exceptions.UnirestException;
import com.mashape.unirest.request.HttpRequestWithBody;

/**
 * @author Varun Kakuste
 */
public class DistributedCacheService implements CacheServiceInterface {
	private String cacheServerUrl;
	private String[] nodes;
	private AtomicInteger successReadCount;
	private AtomicInteger successWriteCount;
	int numOfNodes;

	public DistributedCacheService(String serverUrl) {
		this.cacheServerUrl = serverUrl;
	}

	// Constructor
	public DistributedCacheService(String[] nodes) {
		this.nodes = nodes;
		this.numOfNodes = nodes.length;
	}

	/**
	 * @see edu.sjsu.cmpe.cache.client.CacheServiceInterface#get(long)
	 */
	@Override
	public String get(long key) {
		HttpResponse<JsonNode> response = null;
		try {
			response = Unirest.get(this.cacheServerUrl + "/cache/{key}")
					.header("accept", "application/json")
					.routeParam("key", Long.toString(key)).asJson();
		} catch (UnirestException e) {
			System.err.println(e);
		}
		String value = response.getBody().getObject().getString("value");

		return value;
	}

	/**
	 * @see edu.sjsu.cmpe.cache.client.CacheServiceInterface#put(long,
	 *      java.lang.String)
	 */
	@Override
	public void put(long key, String value) {
		HttpResponse<JsonNode> response = null;
		try {
			response = Unirest
					.put(this.cacheServerUrl + "/cache/{key}/{value}")
					.header("accept", "application/json")
					.routeParam("key", Long.toString(key))
					.routeParam("value", value).asJson();
		} catch (UnirestException e) {
			System.err.println(e);
		}

		if (response.getCode() != 200) {
			System.out.println("Loading to caching failed.");
		}
	}

	/** 
	 * Method to get Values stored using CRDT
	 */
	@Override
	public String getAsynchValues(long key) {
		final CountDownLatch counter = new CountDownLatch(numOfNodes);
		HttpResponse<JsonNode> response = null;
		successReadCount = new AtomicInteger(0);
		final ListMultimap<String, String> successfulServers = ArrayListMultimap.create();
		try {
			for (int itr = 0; itr < numOfNodes; itr++) {
				final String currentServerURL = this.nodes[itr];
				Future<HttpResponse<JsonNode>> future = Unirest
						.get(currentServerURL + "/cache/{key}")
						.header("accept", "application/json")
						.routeParam("key", Long.toString(key))
						.asJsonAsync(new Callback<JsonNode>() {
							String currentUrl = currentServerURL;

							public void failed(UnirestException e) {
								System.out.println("Couldn't process Request...!!!");
							}

							public void completed(HttpResponse<JsonNode> response) {
								int code = response.getCode();
								Headers headers = response.getHeaders();
								JsonNode body = response.getBody();
								InputStream rawBody = response.getRawBody();
								System.out.println(response.getBody());
								String value;
								if (response.getBody() != null) {
									value = response.getBody().getObject().getString("value");
								} else {
									value = "fault";
								}
								successReadCount.incrementAndGet();
								successfulServers.put(value, currentUrl);
								counter.countDown();
							}

							public void cancelled() {
								System.out.println("Request Cancelled...!!!");
							}
						});
			}
			counter.await();
		} catch (Exception expt) {
			System.out.println("Error getting values...!!!");
		}

		List<String> sameValueServers = new ArrayList<String>();
		String successValue = null;

		for (String value : successfulServers.keySet()) {
			List<String> receivedServerUrls = successfulServers.get(value);
			if (receivedServerUrls != null) {
				if (receivedServerUrls.size() > sameValueServers.size()) {
					sameValueServers = receivedServerUrls;
					successValue = value;
				}
			}
		}

		List<String> faultServers = new ArrayList<String>();
		for (String srvUrl : nodes) {
			int itr = 0;
			for (; itr < sameValueServers.size(); itr++) {
				if (srvUrl.equalsIgnoreCase(sameValueServers.get(itr)))
					break;
			}
			if (itr >= sameValueServers.size()) {
				faultServers.add(srvUrl);
			}
		}
		
		System.out.println("Returned Values from Server...!!!");
		for (String srvr : sameValueServers) {
			System.out.println(srvr);
		}
		System.out.println("Repaired values by Server...");
		for (String srvr : faultServers) {
			System.out.println(srvr);
			this.cacheServerUrl = srvr;
			put(key, successValue);
		}
		return successValue;
	}

	/**
	 * Method to Put values using CRDT
	 */
	@Override
	public void insertAsynchValues(long key, String value) {
		try {
			final List<String> successfulServers = new ArrayList<String>();
			successWriteCount = new AtomicInteger(0);
			final CountDownLatch counter = new CountDownLatch(numOfNodes);
			for (int itr = 0; itr < numOfNodes; itr++) {
				final String currentServerURL = this.nodes[itr];
				Future<HttpResponse<JsonNode>> future = Unirest
						.put(currentServerURL + "/cache/{key}/{value}")
						.header("accept", "application/json")
						.routeParam("key", Long.toString(key))
						.routeParam("value", value)
						.asJsonAsync(new Callback<JsonNode>() {
							String currentUrl = currentServerURL;

							public void failed(UnirestException e) {
								System.out.println("Couldn't process...Request Failed...!!!");
								counter.countDown();
							}

							public void completed(
									HttpResponse<JsonNode> response) {
								int code = response.getCode();
								Headers headers = response.getHeaders();
								JsonNode body = response.getBody();
								InputStream rawBody = response.getRawBody();
								successWriteCount.incrementAndGet();
								successfulServers.add(currentUrl);
								counter.countDown();
							}

							public void cancelled() {
								System.out.println("Request Cancelled...!!!");
							}
						});

			}
			counter.await();
			if (numOfNodes % 2 == 0) {
				if (successWriteCount.intValue() >= (numOfNodes / 2)) {
					System.out.println("Added to server...");
					for (String successfulServer : successfulServers) {
						System.out.println(successfulServer);
					}
				} else {
					System.out.println("Deleted from server...");
					for (int i = 0; i < successfulServers.size(); i++) {
						System.out.println(successfulServers.get(i));
						HttpRequestWithBody response = Unirest
								.delete(successfulServers.get(i)
										+ "/cache/{key}");
						System.out.println(response);
					}
				}
			} else {
				if (successWriteCount.intValue() >= ((numOfNodes / 2) + 1)) {
					System.out.println("Added to server...");
					for (String successfulServer : successfulServers) {
						System.out.println(successfulServer);
					}
				} else {
					System.out.println("Deleted from server...");
					for (int i = 0; i < successfulServers.size(); i++) {
						System.out.println(successfulServers.get(i));
						HttpRequestWithBody response = Unirest
								.delete(successfulServers.get(i)
										+ "/cache/{key}");
						System.out.println(response);
					}
				}
			}
		} catch (Exception expt) {
			System.out.println("Error while Processing your reuest...!!!");
		}
	}
}