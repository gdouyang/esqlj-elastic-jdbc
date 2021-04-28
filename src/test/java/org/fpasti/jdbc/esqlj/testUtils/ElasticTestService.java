package org.fpasti.jdbc.esqlj.testUtils;

import java.io.File;
import java.io.IOException;
import java.io.StringReader;
import java.util.UUID;

import org.fpasti.jdbc.esqlj.EsConnection;

import co.elastic.clients.elasticsearch.ElasticsearchClient;
import co.elastic.clients.elasticsearch._types.AcknowledgedResponse;
import co.elastic.clients.elasticsearch.core.CountRequest;
import co.elastic.clients.elasticsearch.core.CountResponse;
import co.elastic.clients.elasticsearch.core.IndexResponse;
import co.elastic.clients.elasticsearch.indices.DeleteTemplateRequest;
import co.elastic.clients.elasticsearch.indices.ExistsRequest;
import co.elastic.clients.elasticsearch.indices.ExistsTemplateRequest;
import co.elastic.clients.elasticsearch.indices.FlushRequest;
import co.elastic.clients.elasticsearch.indices.FlushResponse;
import co.elastic.clients.elasticsearch.indices.PutTemplateRequest;

public class ElasticTestService {

	private static final String RESOURCES_DOCUMENTS = "documents";
	private static final String RESOURCES_TEST_INDEX_TEMPLATE_JSON = "test-index-template.json";
	private static final String ESQLJ_TEST_TEMPLATE = "esqlj-test-template";
	private static final String ELASTIC_BASE_INDEX_CREATE_AND_DESTROY = "esqlj-test-volatile-";
	private static final String ELASTIC_BASE_INDEX_CREATE_ONLY = "esqlj-test-static-020";
	
	public static String CURRENT_INDEX;
	private static Integer NUMBER_OF_DOCS;
	
	public static void setup(EsConnection connection, boolean createAndDestroy) throws Exception {
		cleanUp(connection.getElasticClient());
		setCurrentIndex(createAndDestroy);
		boolean createTemplateAndPostDocs = createAndDestroy ? true : !checkIfStaticIndexJustPresent(connection.getElasticClient());
		if(createTemplateAndPostDocs) {
			addIndexTemplate(connection.getElasticClient());
			postDocuments(connection.getElasticClient(), createAndDestroy);
			flushIndex(connection.getElasticClient());
			awaitForIndexReady();
		}
		
	}

	public static void tearOff(EsConnection connection) throws IOException {
		cleanUp(connection.getElasticClient());
	}

	private static void cleanUp(ElasticsearchClient client) throws IOException {
//		DeleteIndexRequest requestDeleteIndex = new DeleteIndexRequest(ELASTIC_BASE_INDEX_CREATE_AND_DESTROY.concat("*"));        
//		client.indices().delete(requestDeleteIndex, RequestOptions.DEFAULT);
		
		ExistsTemplateRequest request = new ExistsTemplateRequest.Builder().name(ESQLJ_TEST_TEMPLATE).build();
		boolean indexTemplateExists = client.indices().existsTemplate(request).value();
		
		if(indexTemplateExists) {
			DeleteTemplateRequest requestDeleteTemplate = new DeleteTemplateRequest.Builder().name(ESQLJ_TEST_TEMPLATE).build();
			client.indices().deleteTemplate(requestDeleteTemplate);
		}
	}

	private static void addIndexTemplate(ElasticsearchClient client) throws Exception {
		PutTemplateRequest request = new PutTemplateRequest.Builder().name(ESQLJ_TEST_TEMPLATE)
				.withJson(new StringReader(TestUtils.readFile(RESOURCES_TEST_INDEX_TEMPLATE_JSON))).build();
		AcknowledgedResponse res = client.indices().putTemplate(request);
		if(!res.acknowledged()) {
			throw new Exception("Failed to put test template on Elastic instance");
		}
	}
	
	private static void postDocuments(ElasticsearchClient client, boolean createAndDestroy) throws Exception {
		for(File file : TestUtils.listFiles(RESOURCES_DOCUMENTS)) {
			postDocument(client, file.getName().replace(".json", ""), TestUtils.readFile(file), createAndDestroy);
		}
	}

	private static void postDocument(ElasticsearchClient client, String id, String body, boolean createAndDestroy) throws Exception {
		IndexResponse indexResponse = client.index(i -> i.index(CURRENT_INDEX).id(id).withJson(new StringReader(body)));
		if(indexResponse.shards().failed().intValue() > 0) {
			throw new Exception("Failed to insert test document on Elastic instance");
		}
	}
	
	private static void setCurrentIndex(boolean createAndDestroy) {
		CURRENT_INDEX = createAndDestroy ? String.format("%s.%s", ELASTIC_BASE_INDEX_CREATE_AND_DESTROY, UUID.randomUUID()) : ELASTIC_BASE_INDEX_CREATE_ONLY;
	}

	private static boolean checkIfStaticIndexJustPresent(ElasticsearchClient client) throws IOException {
		ExistsRequest request = new ExistsRequest.Builder().index(ELASTIC_BASE_INDEX_CREATE_ONLY).build();
		return client.indices().exists(request).value();
	}

	public static int getNumberOfDocs() {
		if(NUMBER_OF_DOCS == null) {
			CountRequest countRequest = new CountRequest.Builder().index(CURRENT_INDEX).build();
			CountResponse res;
			try {
				res = TestUtils.getLiveConnection().getElasticClient().count(countRequest);
			} catch (Exception e) {
				throw new RuntimeException("Failed to get number of documents on testing index");
			}
			NUMBER_OF_DOCS = new Integer((int)res.count());
		}
		return NUMBER_OF_DOCS;
	}
	
	private static void flushIndex(ElasticsearchClient client) throws Exception {
		FlushRequest request = new FlushRequest.Builder().index(CURRENT_INDEX).waitIfOngoing(true).force(true).build(); 
		FlushResponse res = client.indices().flush(request);
		if(res.shards().failed().intValue() > 0) {
			throw new Exception("Failed to flush test index on Elastic");
		}		
	}
	
	private static void awaitForIndexReady() throws InterruptedException {
		boolean indexNotReady = true;
		int numberOfDocsOnFs = TestUtils.listFiles(RESOURCES_DOCUMENTS).length;
		while(indexNotReady) {
			Thread.sleep(1000);
			indexNotReady = getNumberOfDocs() != numberOfDocsOnFs;
		}
	}

}
