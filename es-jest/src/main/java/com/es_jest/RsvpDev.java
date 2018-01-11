package com.es_jest;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.elasticsearch.search.builder.SearchSourceBuilder;

import com.google.gson.Gson;

import io.searchbox.client.JestClient;
import io.searchbox.client.JestClientFactory;
import io.searchbox.client.JestResult;
import io.searchbox.client.JestResultHandler;
import io.searchbox.client.config.HttpClientConfig;
import io.searchbox.core.Bulk;
import io.searchbox.core.BulkResult;
import io.searchbox.core.Index;
import io.searchbox.core.Search;
import io.searchbox.core.SearchResult;
import io.searchbox.core.SearchResult.Hit;

public class RsvpDev {

	private static Log log = LogFactory.getLog(RsvpDev.class);

	public static void main(String[] args) {
		// TODO Auto-generated method stub
		if (args.length != 4) {
			System.out.println("RsvpDev {sourceUrl} {targetUrl} {sourceIndex} {targetIndex}");
			return;
		}
		final long startTime = System.currentTimeMillis();
		String sourceUrl = args[0];// http://devint-search.qa.rsvphost.com.au
		String targetUrl = args[1];// http://115.159.93.67:9201
		String sourceIndex = args[2];
		String targetIndex = args[3];

		HttpClientConfig clientConfig = new HttpClientConfig.Builder(sourceUrl).readTimeout(6000000).multiThreaded(true).build();
		JestClientFactory factory = new JestClientFactory();
		factory.setHttpClientConfig(clientConfig);
		final JestClient jestClient = factory.getObject();

		HttpClientConfig targetClientConfig = new HttpClientConfig.Builder(targetUrl).readTimeout(6000000).multiThreaded(true).build();
		JestClientFactory targetFactory = new JestClientFactory();
		targetFactory.setHttpClientConfig(targetClientConfig);
		final JestClient targetJestClient = targetFactory.getObject();

		SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
		searchSourceBuilder.query();
		Search search = new Search.Builder(searchSourceBuilder.toString()).addIndex(sourceIndex).build();
		try {
			SearchResult result = jestClient.execute(search);
			long total = result.getJsonObject().getAsJsonObject("hits").get("total").getAsLong();

			int from = 0;
			List<Hit<Map, Void>> hits = null;
			int size = 2000;
			int searchindex = 0;
			final List<Boolean> completeIndicatorList = new ArrayList<Boolean>();
			
			final int totalLoopCount = (int)Math.ceil(1.0d * total / size);
			while (from < total) {
				searchSourceBuilder = new SearchSourceBuilder();
				searchSourceBuilder.from(from);
				searchSourceBuilder.size(size);
				searchSourceBuilder.query();

				search = new Search.Builder(searchSourceBuilder.toString()).addIndex(sourceIndex)
						.build();
				result = jestClient.execute(search);
				hits = result.getHits(Map.class);

				List<Index> indexList = new ArrayList<Index>();

				for (Hit hit : hits) {
					final Map source = (Map) hit.source;

					String id = (String) source.get(JestResult.ES_METADATA_ID);
					Index index = new Index.Builder(source).type(hit.type).id(id).build();
					indexList.add(index);

					/*
					 * final int searchIndex1 = searchindex; targetJestClient.executeAsync(index,
					 * new JestResultHandler<JestResult>() { public void failed(Exception ex) {
					 * System.out.println(searchIndex1 + " failed==>>" + source.get("userid") + ":"
					 * + ExceptionUtils.getStackTrace(ex)); }
					 * 
					 * public void completed(JestResult result) {
					 * 
					 * System.out.println(searchIndex1 + " completed==>>" + source.get("userid")); }
					 * });
					 */

				}

				Bulk bulk = new Bulk.Builder().defaultIndex(targetIndex).addAction(indexList)
						.build();

				/*
				 * BulkResult bulkResult = targetJestClient.execute(bulk);
				 * System.out.println("processed from " + from + " with " + size + " records");
				 */
				final int buldId = searchindex;
				targetJestClient.executeAsync(bulk, new JestResultHandler<BulkResult>() {
					public void failed(Exception ex) {
						completeIndicatorList.add(false);
						System.out.println("Bulk " + buldId + " failed:" + ex);
						if(completeIndicatorList.size() >= totalLoopCount)
						{
							System.out.println("shutdown");
							jestClient.shutdownClient();
							targetJestClient.shutdownClient();
							long endTime = System.currentTimeMillis();
							System.out.println("Spend " + (endTime - startTime) + " to process migration");
						}
					}

					public void completed(BulkResult result) {
					
						//System.out.println(result.getJsonString());
						int spent = result.getJsonObject().get("took").getAsInt();
						completeIndicatorList.add(true);						
						System.out.println("Bulk " + buldId + " complete in " + spent + " millseconds");
						if(completeIndicatorList.size() >= totalLoopCount)
						{
							System.out.println("shutdown");
							jestClient.shutdownClient();
							targetJestClient.shutdownClient();
							long endTime = System.currentTimeMillis();
							System.out.println("Spend " + (endTime - startTime) + " to process migration");
						}
					}
				});

				from += size;
				searchindex++;

			}
			

		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} finally {
			// shutdown client
			//jestClient.shutdownClient();

			//targetJestClient.shutdownClient();
		}
	}

}
