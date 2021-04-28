package org.fpasti.jdbc.esqlj.elastic.query.impl;

import java.io.IOException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.fpasti.jdbc.esqlj.EsConnection;
import org.fpasti.jdbc.esqlj.elastic.model.ElasticSearchableType;
import org.fpasti.jdbc.esqlj.elastic.query.AbstractOneShotQuery;

import co.elastic.clients.elasticsearch.indices.GetAliasRequest;
import co.elastic.clients.elasticsearch.indices.GetAliasResponse;
import co.elastic.clients.elasticsearch.indices.GetIndexRequest;
import co.elastic.clients.elasticsearch.indices.GetIndexResponse;

/**
* @author  Fabrizio Pasti - fabrizio.pasti@gmail.com
*/

public class IndicesQuery extends AbstractOneShotQuery {
			
	private static String[] COLUMNS =  {"TABLE_CAT", "TABLE_SCHEM", "TABLE_NAME", "TABLE_TYPE", "REMARKS", "TYPE_CAT", "TYPE_SCHEM", "TYPE_NAME", "SELF_REFERENCING_COL_NAME", "REF_GENERATION"};
	
	public IndicesQuery(EsConnection connection, ElasticSearchableType... types) throws SQLException {
		super(connection, "system_indices", COLUMNS);
		
		if(Arrays.asList(types).contains(ElasticSearchableType.INDEX)) {
			init(ElasticSearchableType.INDEX);
		}
		
		if(Arrays.asList(types).contains(ElasticSearchableType.ALIAS)) {
			init(ElasticSearchableType.ALIAS);
		}
	}

	public void init(ElasticSearchableType type) throws SQLException {
		List<String> indices = type == ElasticSearchableType.INDEX ? retrieveIndices() : retrieveAliases();
		indices.forEach(indice -> {
			Map<String, Object> data = new HashMap<String, Object>();
			data.put("TABLE_NAME", indice);
			data.put("TABLE_TYPE", type == ElasticSearchableType.INDEX ? "TABLE" : "VIEW");
			data.put("REMARKS", "");
			insertRowWithData(data);
		});
	}
	
	private List<String> retrieveIndices() throws SQLException {
		try {
			GetIndexRequest indexRequest = new GetIndexRequest.Builder().index("*").build();
			GetIndexResponse indexResponse = getConnection().getElasticClient().indices().get(indexRequest);
			List<String> indices = new ArrayList<String>(indexResponse.result().keySet());
			return indices.stream().sorted().collect(Collectors.toList());
		} catch(IOException e) {
			throw new SQLException("Failed to retrieve indices and aliases from Elastic");
		}
	}

	private List<String> retrieveAliases() throws SQLException {
		try {
			GetAliasRequest indexRequest = new GetAliasRequest.Builder().index("*").build();
			GetAliasResponse indexResponse = getConnection().getElasticClient().indices().getAlias(indexRequest);
			List<String> aliases = new ArrayList<String>(indexResponse.result().keySet());
			return aliases.stream().sorted().collect(Collectors.toList());
		} catch(IOException e) {
			throw new SQLException("Failed to retrieve indices and aliases from Elastic");
		}
	}
}
