package org.fpasti.jdbc.esqlj.elastic.query.impl.search;

import java.sql.SQLException;
import java.sql.SQLNonTransientConnectionException;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import org.fpasti.jdbc.esqlj.Configuration;
import org.fpasti.jdbc.esqlj.ConfigurationPropertyEnum;
import org.fpasti.jdbc.esqlj.EsConnection;
import org.fpasti.jdbc.esqlj.EsMetaData;
import org.fpasti.jdbc.esqlj.elastic.metadata.ElasticServerDetails;
import org.fpasti.jdbc.esqlj.elastic.metadata.MetaDataService;
import org.fpasti.jdbc.esqlj.elastic.model.ElasticFieldType;
import org.fpasti.jdbc.esqlj.elastic.model.ElasticObject;
import org.fpasti.jdbc.esqlj.elastic.model.IndexMetaData;
import org.fpasti.jdbc.esqlj.elastic.query.data.PageDataElastic;
import org.fpasti.jdbc.esqlj.elastic.query.model.PaginationType;
import org.fpasti.jdbc.esqlj.elastic.query.statement.SqlStatementSelect;
import org.fpasti.jdbc.esqlj.elastic.query.statement.model.QueryType;
import co.elastic.clients.elasticsearch._types.FieldValue;
import co.elastic.clients.elasticsearch._types.Time;
import co.elastic.clients.elasticsearch.core.SearchRequest;
import co.elastic.clients.elasticsearch.core.search.Hit;
import co.elastic.clients.elasticsearch.core.search.PointInTimeReference;
import co.elastic.clients.elasticsearch.core.search.ResponseBody;

/**
* @author  Fabrizio Pasti - fabrizio.pasti@gmail.com
*/

public class RequestInstance {

	private Map<String, ElasticObject> fields;
	private List<String> columnNames;
	private SearchRequest.Builder searchSourceBuilder;
	private int fetchSize;
	private PaginationType paginationMode = PaginationType.NO_SCROLL;
	private String paginationId;
	private MetaDataService metaDataService;
	private IndexMetaData indexMetaData;
	private boolean pointInTimeApiAvailable;
	private List<FieldValue> paginationSortValues;
	private boolean scrollOpen;
	private SqlStatementSelect select;
	
	Pattern pattern = Pattern.compile("\"id\":\\s*\"([\\w=]*)\"");
	
	public RequestInstance(EsConnection connection, int fetchSize, SqlStatementSelect select) throws SQLException {
		this.metaDataService = ((EsMetaData)connection.getMetaData()).getMetaDataService();
		this.select = select;
		this.indexMetaData = metaDataService.getIndexMetaData(select.getIndex().getName());
		searchSourceBuilder = new SearchRequest.Builder().index(select.getIndex().getName());
		this.fetchSize = fetchSize;
		implementScrollStrategy();
		checkPointInTimeWorkAround(connection); // hey Elastic team! Where is the api for point in time search?
	}

	public IndexMetaData getIndexMetaData() {
		return indexMetaData;
	}
	
	public Map<String, ElasticObject> getFields() {
		return fields;
	}

	public void setFields(Map<String, ElasticObject> fields) {
		this.fields = fields;
	}
	
	public List<String> getColumnNames() {
		return columnNames != null ? columnNames : getFieldNames();
	}

	public void setColumnNames(List<String> columnNames) {
		this.columnNames = columnNames;
	}

	public List<String> getFieldNames() {
		return fields.keySet().stream().collect(Collectors.toList());
	}

	public List<ElasticFieldType> getFieldTypes() {
		return fields.entrySet().stream().map(field -> field.getValue().getType()).collect(Collectors.toList());
	}

	public SearchRequest getSearchRequest() {
		return searchSourceBuilder.build();
	}

	public SearchRequest.Builder getSearchSourceBuilder() {
		return searchSourceBuilder;
	}

	public boolean isStarSelect() {
		return select.getQueryColumns().size() == 1 && select.getQueryColumns().get(0).getName().equals("*");
	}
	
	public boolean isSourceFieldsToRetrieve() {
		return !isStarSelect() || Configuration.getConfiguration(ConfigurationPropertyEnum.CFG_INCLUDE_TEXT_FIELDS_BY_DEFAULT, Boolean.class);
	}

	public PaginationType getPaginationMode() {
		return paginationMode;
	}
	
	public boolean isScrollable() {
		return !paginationMode.equals(PaginationType.NO_SCROLL);
	}
	
	public boolean isOrdered() {
		return true;
	}
		
	public int getFetchSize() {
		return fetchSize;
	}
	
	public String getPaginationId() {
		return paginationId;
	}

	public void setPaginationId(String paginationId) {
		this.paginationId = paginationId;
	}

	private void implementScrollStrategy() {
		if(!select.getQueryType().equals(QueryType.DOCS)) {
			paginationMode = PaginationType.NO_SCROLL;
			return;
		}
		
		if(select.getLimit() != null && select.getLimit() < Configuration.getConfiguration(ConfigurationPropertyEnum.CFG_QUERY_SCROLL_FROM_ROWS, Long.class)) {
			return;
		}
		
		if(isOrdered() && !Configuration.getConfiguration(ConfigurationPropertyEnum.CFG_QUERY_SCROLL_ONLY_BY_SCROLL_API, Boolean.class)) {
			paginationMode = metaDataService.getElasticServerDetails().isElasticReleaseEqOrGt(ElasticServerDetails.ELASTIC_REL_7_10_0) && pointInTimeApiAvailable ? PaginationType.BY_ORDER_WITH_PIT : PaginationType.BY_ORDER;
		} else {
			paginationMode = PaginationType.SCROLL_API;
		}
		
		setScrollOpen(true);
	}

	public void updateRequest(ResponseBody<?> searchResponse, PageDataElastic pageData) throws SQLNonTransientConnectionException {
		updateFetchSize(pageData);
		updatePagination(searchResponse);
	}

	public SqlStatementSelect getSelect() {
		return select;
	}

	public boolean isScrollOpen() {
		return scrollOpen;
	}

	public void setScrollOpen(boolean scrollOpen) {
		this.scrollOpen = scrollOpen;
	}
	
	private void updateFetchSize(PageDataElastic pageData) {
		if(select.getLimit() != null) {
			searchSourceBuilder.size((pageData.getFetchedRows() + fetchSize) > select.getLimit() ? new Long(select.getLimit() - pageData.getFetchedRows()).intValue() : fetchSize);
		}
	}
	
	@SuppressWarnings("incomplete-switch")
	private void updatePagination(ResponseBody<?> searchResponse) throws SQLNonTransientConnectionException {
		switch(paginationMode) {
			case SCROLL_API:
				paginationId = searchResponse.scrollId();
				break;
			case BY_ORDER:
				updateSearchAfter(searchResponse);
				break;
			case BY_ORDER_WITH_PIT:
				updateSearchAfter(searchResponse);
				Long configuration = Configuration.getConfiguration(ConfigurationPropertyEnum.CFG_QUERY_SCROLL_TIMEOUT_MINUTES, Long.class);
	            Time time = new Time.Builder().time(String.format("%dm", configuration)).build();
	            PointInTimeReference pit = new PointInTimeReference.Builder()
	                .id(searchResponse.pitId())
	                .keepAlive(time)
	                .build();
				getSearchSourceBuilder().pit(pit);
				break;
		}
	}

	private void updateSearchAfter(ResponseBody<?> searchResponse) {
		if(searchResponse.hits().hits().size() > 0) {
			Hit<?> object = searchResponse.hits().hits().get(searchResponse.hits().hits().size() - 1);
			paginationSortValues = object.sort();
			searchSourceBuilder.searchAfter(paginationSortValues);
		}
	}
	
	private void checkPointInTimeWorkAround(EsConnection connection) {
		try {
			connection.getElasticClient().migration();
		} catch(Exception e) {
			pointInTimeApiAvailable = true;
		}
	}
	
}
