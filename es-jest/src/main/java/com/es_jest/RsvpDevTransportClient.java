package com.es_jest;

import java.net.InetAddress;
import java.util.Map;

import org.apache.commons.lang3.math.NumberUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.elasticsearch.action.bulk.BackoffPolicy;
import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkProcessor;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.transport.client.PreBuiltTransportClient;

import io.searchbox.client.JestClient;
import io.searchbox.client.JestClientFactory;
import io.searchbox.client.JestResult;
import io.searchbox.client.config.HttpClientConfig;
import io.searchbox.core.Search;
import io.searchbox.core.SearchResult;
import io.searchbox.strings.StringUtils;

public class RsvpDevTransportClient {

	private static String mapping = "{\"blockees\":{\"type\":\"long\"},\"bodytype\":{\"type\":\"long\"},\"bodytypestr\":{\"type\":\"text\",\"fields\":{\"keyword\":{\"type\":\"keyword\",\"ignore_above\":256}}},\"completionscore\":{\"type\":\"long\"},\"countryid\":{\"type\":\"long\"},\"countrystr\":{\"type\":\"text\",\"fields\":{\"keyword\":{\"type\":\"keyword\",\"ignore_above\":256}}},\"created\":{\"type\":\"date\"},\"diet\":{\"type\":\"long\"},\"dietstr\":{\"type\":\"text\",\"fields\":{\"keyword\":{\"type\":\"keyword\",\"ignore_above\":256}}},\"divisionid\":{\"type\":\"long\"},\"divisionstr\":{\"type\":\"text\",\"fields\":{\"keyword\":{\"type\":\"keyword\",\"ignore_above\":256}}},\"dob\":{\"type\":\"date\"},\"docdatetime\":{\"type\":\"date\"},\"drink\":{\"type\":\"long\"},\"drinkstr\":{\"type\":\"text\",\"fields\":{\"keyword\":{\"type\":\"keyword\",\"ignore_above\":256}}},\"education\":{\"type\":\"long\"},\"educationstr\":{\"type\":\"text\",\"fields\":{\"keyword\":{\"type\":\"keyword\",\"ignore_above\":256}}},\"es_metadata_id\":{\"type\":\"text\",\"fields\":{\"keyword\":{\"type\":\"keyword\",\"ignore_above\":256}}},\"ethnicbackground\":{\"type\":\"long\"},\"ethnicbackgroundstr\":{\"type\":\"text\",\"fields\":{\"keyword\":{\"type\":\"keyword\",\"ignore_above\":256}}},\"ethnicity\":{\"type\":\"text\",\"fields\":{\"keyword\":{\"type\":\"keyword\",\"ignore_above\":256}}},\"eyecolor\":{\"type\":\"long\"},\"eyecolorstr\":{\"type\":\"text\",\"fields\":{\"keyword\":{\"type\":\"keyword\",\"ignore_above\":256}}},\"freshnesschunk0\":{\"type\":\"long\"},\"freshnesschunk1\":{\"type\":\"long\"},\"freshnesschunk2\":{\"type\":\"long\"},\"freshnesschunk3\":{\"type\":\"long\"},\"freshnesschunk4\":{\"type\":\"long\"},\"freshnesschunk5\":{\"type\":\"long\"},\"freshnesschunk6\":{\"type\":\"long\"},\"freshnessday\":{\"type\":\"long\"},\"gender\":{\"type\":\"long\"},\"genderstr\":{\"type\":\"text\",\"fields\":{\"keyword\":{\"type\":\"keyword\",\"ignore_above\":256}}},\"haircolor\":{\"type\":\"long\"},\"haircolorstr\":{\"type\":\"text\",\"fields\":{\"keyword\":{\"type\":\"keyword\",\"ignore_above\":256}}},\"havechildren\":{\"type\":\"long\"},\"havechildrenstr\":{\"type\":\"text\",\"fields\":{\"keyword\":{\"type\":\"keyword\",\"ignore_above\":256}}},\"havepets\":{\"type\":\"long\"},\"havepetsstr\":{\"type\":\"text\",\"fields\":{\"keyword\":{\"type\":\"keyword\",\"ignore_above\":256}}},\"headline\":{\"type\":\"text\",\"fields\":{\"keyword\":{\"type\":\"keyword\",\"ignore_above\":256}}},\"height\":{\"type\":\"long\"},\"idealpartner\":{\"type\":\"text\",\"fields\":{\"keyword\":{\"type\":\"keyword\",\"ignore_above\":256}}},\"ignorers\":{\"type\":\"long\"},\"interests\":{\"type\":\"text\",\"fields\":{\"keyword\":{\"type\":\"keyword\",\"ignore_above\":256}}},\"ipbodytype\":{\"type\":\"long\"},\"ipcountryids\":{\"type\":\"long\"},\"ipdiet\":{\"type\":\"long\"},\"ipdivisionids\":{\"type\":\"long\"},\"ipdrink\":{\"type\":\"long\"},\"ipeducation\":{\"type\":\"long\"},\"ipethnicbackground\":{\"type\":\"long\"},\"ipeyecolor\":{\"type\":\"long\"},\"iphaircolor\":{\"type\":\"long\"},\"iphasphoto\":{\"type\":\"long\"},\"iphavechildren\":{\"type\":\"long\"},\"iphavepets\":{\"type\":\"long\"},\"ipheight\":{\"type\":\"long\"},\"iplocuspointids\":{\"type\":\"long\"},\"ipmaritalstatus\":{\"type\":\"long\"},\"ipmaxage\":{\"type\":\"long\"},\"ipmaxlat\":{\"type\":\"float\"},\"ipmaxlon\":{\"type\":\"float\"},\"ipminage\":{\"type\":\"long\"},\"ipminlat\":{\"type\":\"float\"},\"ipminlon\":{\"type\":\"float\"},\"ipoccindustry\":{\"type\":\"long\"},\"ipocclevel\":{\"type\":\"long\"},\"ippersonality\":{\"type\":\"long\"},\"ippolitics\":{\"type\":\"long\"},\"ipreligion\":{\"type\":\"long\"},\"ipsmoke\":{\"type\":\"long\"},\"ipstarsign\":{\"type\":\"long\"},\"ipwantchildren\":{\"type\":\"long\"},\"language\":{\"type\":\"text\",\"fields\":{\"keyword\":{\"type\":\"keyword\",\"ignore_above\":256}}},\"location\":{\"type\":\"geo_point\"},\"locationid\":{\"type\":\"long\"},\"locuspointid\":{\"type\":\"long\"},\"locuspointstr\":{\"type\":\"text\",\"fields\":{\"keyword\":{\"type\":\"keyword\",\"ignore_above\":256}}},\"maintext\":{\"type\":\"text\",\"fields\":{\"keyword\":{\"type\":\"keyword\",\"ignore_above\":256}}},\"maritalstatus\":{\"type\":\"long\"},\"maritalstatusstr\":{\"type\":\"text\",\"fields\":{\"keyword\":{\"type\":\"keyword\",\"ignore_above\":256}}},\"movies\":{\"type\":\"text\",\"fields\":{\"keyword\":{\"type\":\"keyword\",\"ignore_above\":256}}},\"music\":{\"type\":\"text\",\"fields\":{\"keyword\":{\"type\":\"keyword\",\"ignore_above\":256}}},\"occupationindustry\":{\"type\":\"long\"},\"occupationindustrystr\":{\"type\":\"text\",\"fields\":{\"keyword\":{\"type\":\"keyword\",\"ignore_above\":256}}},\"occupationlevel\":{\"type\":\"long\"},\"occupationlevelstr\":{\"type\":\"text\",\"fields\":{\"keyword\":{\"type\":\"keyword\",\"ignore_above\":256}}},\"personality\":{\"type\":\"long\"},\"personalitystr\":{\"type\":\"text\",\"fields\":{\"keyword\":{\"type\":\"keyword\",\"ignore_above\":256}}},\"photostatus\":{\"type\":\"long\"},\"politics\":{\"type\":\"long\"},\"politicsstr\":{\"type\":\"text\",\"fields\":{\"keyword\":{\"type\":\"keyword\",\"ignore_above\":256}}},\"psaffiliativehumour\":{\"type\":\"long\"},\"psaggressivehumour\":{\"type\":\"long\"},\"psagreeableness\":{\"type\":\"long\"},\"psavoidingconflict\":{\"type\":\"long\"},\"pscommunication\":{\"type\":\"long\"},\"pscompromisingconflict\":{\"type\":\"long\"},\"psconscientiousness\":{\"type\":\"long\"},\"psdismissive\":{\"type\":\"long\"},\"psdominatingconflict\":{\"type\":\"long\"},\"pseat\":{\"type\":\"long\"},\"psemotions\":{\"type\":\"long\"},\"psexpenditure\":{\"type\":\"long\"},\"psextraversion\":{\"type\":\"long\"},\"psfearful\":{\"type\":\"long\"},\"pshighschool\":{\"type\":\"long\"},\"psholiday\":{\"type\":\"long\"},\"pshonesty\":{\"type\":\"long\"},\"pshousehold\":{\"type\":\"long\"},\"psidealtidiness\":{\"type\":\"long\"},\"psintegratingconflict\":{\"type\":\"long\"},\"psmoney\":{\"type\":\"long\"},\"psmovies\":{\"type\":\"long\"},\"psmusic\":{\"type\":\"long\"},\"psneuroticism\":{\"type\":\"long\"},\"psobligingconflict\":{\"type\":\"long\"},\"psopenness\":{\"type\":\"long\"},\"psoptimism\":{\"type\":\"long\"},\"pspreoccupied\":{\"type\":\"long\"},\"psromance\":{\"type\":\"long\"},\"pssecure\":{\"type\":\"long\"},\"psselfdefeatinghumour\":{\"type\":\"long\"},\"psselfenhancinghumour\":{\"type\":\"long\"},\"psselfesteem\":{\"type\":\"long\"},\"pssex\":{\"type\":\"long\"},\"pstidiness\":{\"type\":\"long\"},\"psweekend\":{\"type\":\"long\"},\"pswork\":{\"type\":\"long\"},\"reading\":{\"type\":\"text\",\"fields\":{\"keyword\":{\"type\":\"keyword\",\"ignore_above\":256}}},\"religion\":{\"type\":\"long\"},\"religionstr\":{\"type\":\"text\",\"fields\":{\"keyword\":{\"type\":\"keyword\",\"ignore_above\":256}}},\"responserate\":{\"type\":\"long\"},\"romanticgendertarget\":{\"type\":\"long\"},\"secondaryphotostatus\":{\"type\":\"long\"},\"sexuality\":{\"type\":\"long\"},\"sexualitystr\":{\"type\":\"text\",\"fields\":{\"keyword\":{\"type\":\"keyword\",\"ignore_above\":256}}},\"smoke\":{\"type\":\"long\"},\"smokestr\":{\"type\":\"text\",\"fields\":{\"keyword\":{\"type\":\"keyword\",\"ignore_above\":256}}},\"sport\":{\"type\":\"text\",\"fields\":{\"keyword\":{\"type\":\"keyword\",\"ignore_above\":256}}},\"starsign\":{\"type\":\"long\"},\"starsignstr\":{\"type\":\"text\",\"fields\":{\"keyword\":{\"type\":\"keyword\",\"ignore_above\":256}}},\"subexpiry\":{\"type\":\"date\"},\"subexpirygold\":{\"type\":\"date\"},\"userid\":{\"type\":\"long\"},\"username\":{\"type\":\"text\",\"fields\":{\"keyword\":{\"type\":\"keyword\",\"ignore_above\":256}}},\"usernamealpha\":{\"type\":\"text\",\"fields\":{\"keyword\":{\"type\":\"keyword\",\"ignore_above\":256}}},\"visible\":{\"type\":\"long\"},\"wantchildren\":{\"type\":\"long\"},\"wantchildrenstr\":{\"type\":\"text\",\"fields\":{\"keyword\":{\"type\":\"keyword\",\"ignore_above\":256}}}}";

	private static Log log = LogFactory.getLog(RsvpDevTransportClient.class);
	public static void main(String[] args) {
		try {
			
			String sourceHost = System.getProperty("sourceHost");
			String sourceIndex = System.getProperty("sourceIndex");
			String sourcePortStr = System.getProperty("sourcePort");
			
			String targetHost = System.getProperty("targetHost");;
			String targetIndex = System.getProperty("targetIndex");
			String targetPortStr = System.getProperty("targetPort");
			
			String fromTimeStr = System.getProperty("fromTime");
			
			boolean hasError = false;
			if (StringUtils.isBlank(sourceHost))
			{
				hasError = true;
				System.out.println("Please provide the source host");
			}
			
			if (StringUtils.isBlank(sourceIndex))
			{
				hasError = true;
				System.out.println("Please provide the source index");
			}
			
			if (StringUtils.isBlank(targetHost))
			{
				hasError = true;
				System.out.println("Please provide the target host");
			}
			
			if (!StringUtils.isBlank(sourcePortStr) && !NumberUtils.isDigits(sourcePortStr) )
			{
				hasError = true;
				System.out.println("Please provide correct source port");
			}
			
			if (!StringUtils.isBlank(targetPortStr) && !NumberUtils.isDigits(targetPortStr) )
			{
				hasError = true;
				System.out.println("Please provide correct target port");
			}
			
			
			if (hasError)
			{
				return;
			}
			
			log.info("Start to do migration from " + sourceHost + " to " + targetHost);
			
			if (StringUtils.isBlank(targetIndex))
			{
				targetIndex = sourceIndex;
			}
			
			int targetPort = 9300;
			if (!StringUtils.isBlank(targetPortStr))
			{
				targetPort = Integer.valueOf(targetPortStr);
			}
			String sourceUrl = "http://" + sourceHost;
			if (!StringUtils.isBlank(sourcePortStr))
			{
				sourceUrl += ":" + Integer.valueOf(sourcePortStr);
			}
			
			HttpClientConfig clientConfig = new HttpClientConfig.Builder(sourceUrl)
					.multiThreaded(true).build();
			JestClientFactory factory = new JestClientFactory();
			factory.setHttpClientConfig(clientConfig);
			JestClient jestClient = factory.getObject();

			TransportClient targetClient = new PreBuiltTransportClient(Settings.EMPTY)
					.addTransportAddress(new TransportAddress(InetAddress.getByName(targetHost), targetPort));

			SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
			searchSourceBuilder.query();
			Search search = new Search.Builder(searchSourceBuilder.toString()).addIndex(sourceIndex).addType("profile")
					.build();

			JestResult result = jestClient.execute(search);
			long total = result.getJsonObject().getAsJsonObject("hits").get("total").getAsLong();
			int from = 0;
			int size = 1000;

			BulkProcessor bulkProcessor = BulkProcessor.builder(targetClient, new BulkProcessor.Listener() {
				public void beforeBulk(long executionId, BulkRequest request) {
					System.out.println("Before Bulkd ID " + executionId);
				}

				public void afterBulk(long executionId, BulkRequest request, BulkResponse response) {
					System.out.println("bulk id " + executionId + " succeed with " + response.getItems().length);
					log.info("bulk " + executionId + " spend " + response.getTook().getMillis() + " millseconds");
				}

				public void afterBulk(long executionId, BulkRequest request, Throwable failure) {
					System.out.println("bulk id " + executionId + " failed " + failure.getMessage());
				}
			}).setBulkActions(10000).setBulkSize(new ByteSizeValue(5, ByteSizeUnit.MB))
					.setFlushInterval(TimeValue.timeValueSeconds(30)).setConcurrentRequests(2)
					.setBackoffPolicy(BackoffPolicy.exponentialBackoff(TimeValue.timeValueMillis(100), 3)).build();

			while (from < total) {
				searchSourceBuilder = new SearchSourceBuilder();
				searchSourceBuilder.from(from);
				searchSourceBuilder.size(size);
				searchSourceBuilder.query();

				log.info("search from " + from + " with " + size + " records");
				search = new Search.Builder(searchSourceBuilder.toString()).addIndex(sourceIndex).addType("profile")
						.build();
				SearchResult jestResult = jestClient.execute(search);
				log.info("spend " + jestResult.getJsonMap().get("took") + " millseconds for this search");
				int searchTotal = jestResult.getHits(Map.class).size();

				
				//BulkRequestBuilder bulkRequest = targetClient.prepareBulk();

				for (int i = 0; i < searchTotal; i++) {
					String id = jestResult.getHits(Map.class).get(i).id;

					/*
					bulkRequest.add(targetClient.prepareIndex("rsvpdev", "profile", id)
							.setSource(jestResult.getHits(Map.class).get(i).source));
							*/
					bulkProcessor.add(targetClient.prepareIndex(targetIndex, "profile", id)
							.setSource(jestResult.getHits(Map.class).get(i).source).request());

				}

				/*
				BulkResponse bulkResponse = bulkRequest.get();
				for (BulkItemResponse item : bulkResponse.getItems()) {
					System.out.println("complete =>" + item.getId());
				}
				*/

				from += size;

			}
			
			bulkProcessor.close();

			/*
			 * QueryBuilder qb = QueryBuilders.matchAllQuery(); SearchResponse scrollResp =
			 * client.prepareSearch("rsvpold")
			 * .setTypes("profile").setSearchType(SearchType.DFS_QUERY_THEN_FETCH)
			 * .setQuery(qb).setFrom(0).setSize(2000) .get();
			 * 
			 * long total = scrollResp.getHits().getHits().length;
			 * 
			 * for (int i = 0; i<total; i++) { SearchHit hit =
			 * scrollResp.getHits().getAt(i); String source = hit.getSourceAsString(); Map
			 * map = hit.getSourceAsMap();
			 * 
			 * IndexResponse response = targetClient.prepareIndex("rsvpdev", "profile",
			 * hit.getId()).setSource(map).get(); System.out.println("complete =>" +
			 * hit.getId()); }
			 * 
			 */

			jestClient.shutdownClient();
			targetClient.close();
			
			log.info("Completed the migration");
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}
}
