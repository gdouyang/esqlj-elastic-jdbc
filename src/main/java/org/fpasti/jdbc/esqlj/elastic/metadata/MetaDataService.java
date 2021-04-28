package org.fpasti.jdbc.esqlj.elastic.metadata;

import java.io.IOException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.stream.Collectors;

import org.fpasti.jdbc.esqlj.Configuration;
import org.fpasti.jdbc.esqlj.ConfigurationPropertyEnum;
import org.fpasti.jdbc.esqlj.elastic.model.ElasticFieldType;
import org.fpasti.jdbc.esqlj.elastic.model.ElasticObject;
import org.fpasti.jdbc.esqlj.elastic.model.IndexMetaData;

import co.elastic.clients.elasticsearch.ElasticsearchClient;
import co.elastic.clients.elasticsearch._types.mapping.FieldMapping;
import co.elastic.clients.elasticsearch._types.mapping.Property;
import co.elastic.clients.elasticsearch.core.InfoResponse;
import co.elastic.clients.elasticsearch.indices.GetFieldMappingRequest;
import co.elastic.clients.elasticsearch.indices.GetFieldMappingResponse;

/**
* @author  Fabrizio Pasti - fabrizio.pasti@gmail.com
*/

public class MetaDataService {
	private ElasticServerDetails elasticServerDetails;
	private ElasticsearchClient client;
	private Map<String, IndexMetaData> cacheIndexMetaData = new HashMap<String, IndexMetaData>();
	
	public MetaDataService(ElasticsearchClient client) throws SQLException {
		this.client = client;
		retrieveElasticInfo();
	}

	private void retrieveElasticInfo() throws SQLException {
		try {
			InfoResponse response = client.info();
			
			setElasticServerDetails(new ElasticServerDetails(response));
			
		} catch (IOException e) {
			throw new SQLException("Failed to retrieve info from Elastic");
		}
	}

	public String getProductName() {
		return "Elasticsearch";
	}
	
	public ElasticServerDetails getElasticServerDetails() {
		return elasticServerDetails;
	}

	public void setElasticServerDetails(ElasticServerDetails elasticServerDetails) {
		this.elasticServerDetails = elasticServerDetails;
	}

	public IndexMetaData getIndexMetaData(String index) throws SQLException {
		if(Configuration.getConfiguration(ConfigurationPropertyEnum.CFG_INDEX_METADATA_CACHE, Boolean.class)) {
			synchronized(cacheIndexMetaData) {
				if(!cacheIndexMetaData.containsKey(index)) {
					cacheIndexMetaData.put(index, new IndexMetaData(index, getIndexFields(index)));
				}
			}
			
			return cacheIndexMetaData.get(index);
		}

		return new IndexMetaData(index, getIndexFields(index));
	}
	
	public Map<String, ElasticObject> getIndexFields(String index) throws SQLException {
		try {
			GetFieldMappingRequest request = new GetFieldMappingRequest.Builder().index(index).fields("*").build();
			
			GetFieldMappingResponse response = client.indices().getFieldMapping(request);
	
			Map<String, ElasticObject> fields = new TreeMap<String, ElasticObject>();
			List<String> managedFields = new ArrayList<String>();
			response.result().forEach((indexName, metadata) -> {
				Map<String, FieldMapping> mappings = metadata.mappings();
				mappings.forEach((field, fm) -> {
					Map<String, Property> metadataMap = fm.mapping();
					if(metadataMap.size() > 0 && !managedFields.stream().anyMatch(field::equals)) {
						Property fieldMap = metadataMap.get(field.substring(field.lastIndexOf('.') + 1));
						ElasticFieldType fieldType = ElasticFieldType.resolveByElasticType(fieldMap._kind().jsonValue());
						
						ElasticObject elasticObject = new ElasticObject(field, fieldType, null, isDocValue(fieldType));
						if (fieldMap.isKeyword()) {
							Boolean docValues = fieldMap.keyword().docValues();
							if (docValues != null) {
								elasticObject.setDocValue(docValues);
							}
							Integer ignoreAbove = fieldMap.keyword().ignoreAbove();
							if (ignoreAbove != null) {
								elasticObject.setSize(ignoreAbove.longValue());
							}
						}
						fields.put(field, elasticObject);
						managedFields.add(field);
					}
				});
			});
			
			return fields.entrySet().stream()
					.sorted(Map.Entry.comparingByKey())
					.collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue,(oldValue, newValue) -> oldValue, LinkedHashMap::new));
		} catch(IOException e) {
			throw new SQLException(e.getMessage());
		}
	}

	private boolean isDocValue(ElasticFieldType fieldType) {
		switch(fieldType) {
			case TEXT:
				return false;
			case BINARY:
				return false;
			default:
				return true;
		}
	}
	
	
}
