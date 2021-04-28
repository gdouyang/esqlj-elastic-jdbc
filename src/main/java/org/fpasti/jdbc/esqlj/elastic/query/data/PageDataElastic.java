package org.fpasti.jdbc.esqlj.elastic.query.data;

import java.sql.SQLException;
import java.text.ParseException;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TimeZone;
import org.fpasti.jdbc.esqlj.elastic.model.ElasticObject;
import org.fpasti.jdbc.esqlj.elastic.model.EsGeoPoint;
import org.fpasti.jdbc.esqlj.elastic.query.impl.search.RequestInstance;
import org.fpasti.jdbc.esqlj.elastic.query.model.DataRow;
import org.fpasti.jdbc.esqlj.elastic.query.model.PageDataState;
import org.fpasti.jdbc.esqlj.elastic.query.statement.model.QueryColumn;
import org.fpasti.jdbc.esqlj.support.SimpleDateFormatThreadSafe;
import co.elastic.clients.elasticsearch._types.aggregations.Aggregate;
import co.elastic.clients.elasticsearch._types.aggregations.DoubleTermsBucket;
import co.elastic.clients.elasticsearch._types.aggregations.LongTermsBucket;
import co.elastic.clients.elasticsearch._types.aggregations.MultiBucketAggregateBase;
import co.elastic.clients.elasticsearch._types.aggregations.StringTermsBucket;
import co.elastic.clients.elasticsearch.core.search.Hit;
import co.elastic.clients.elasticsearch.core.search.ResponseBody;
import co.elastic.clients.json.JsonData;

/**
* @author  Fabrizio Pasti - fabrizio.pasti@gmail.com
*/

public class PageDataElastic {
	
	private PageDataState state = PageDataState.NOT_INITIALIZED;
	private int currentIdxCurrentRow = -1;
	private int iterationStep = 1;
	private RequestInstance req;
	private Long fetchedRows = 0L;
	
	private List<DataRow> dataRows;

	public static final SimpleDateFormatThreadSafe sdfTimestamp = new SimpleDateFormatThreadSafe("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'", TimeZone.getTimeZone("UTC"));
	
	public PageDataElastic(String source, RequestInstance req) {
		this.req = req;
	}

	public void pushData(ResponseBody<?> searchResponse) {
		currentIdxCurrentRow = -1;
		
		switch(req.getSelect().getQueryType()) {
			case DOCS:
				pushDocuments(searchResponse);
				break;
			case AGGR_COUNT_ALL:
				manageCountAll(searchResponse);
				break;
			case AGGR_UNGROUPED_EXPRESSIONS:
				manageUngroupedExpression(searchResponse);
				break;
			case DISTINCT_DOCS:
			case AGGR_GROUP_BY:
				manageGroupBy(searchResponse);
				break;
		}
	}

	private void pushDocuments(ResponseBody<?> res) {
		boolean firstPush = dataRows == null;
			
		if(!firstPush) {
			DataRow dataRow = dataRows.get(dataRows.size() - 1);
			dataRows = new ArrayList<DataRow>();
			dataRows.add(dataRow);
		} else {
			dataRows = new ArrayList<DataRow>();
		}
		
		int takeNRows = req.getSelect().getLimit() != null ? (res.hits().hits().size() + fetchedRows > req.getSelect().getLimit() ? new Long(req.getSelect().getLimit() - fetchedRows).intValue() : res.hits().hits().size()) : res.hits().hits().size();

		for(int i = 0; i < takeNRows; i++) {
			Hit<?> searchHit = res.hits().hits().get(i);
			List<Object> data = new ArrayList<Object>();
			Map<String, JsonData> fields = searchHit.fields();
			req.getFields().forEach((name, field) -> {
				if(field.isDocValue()) {
				    JsonData docField = fields.get(field.getFullName());
					if(docField != null) {
					    List<?> list = docField.to(List.class);
					    if (list != null && list.size() > 0) {
					      data.add(resolveField(field, list.get(0))); // only first field value is managed
					    } else {
	                        data.add(null);
	                    }
					} else {
						data.add(null);
					}
				} else if(field.isSourceField() && req.isSourceFieldsToRetrieve()) {
					data.add(((Map)searchHit.source()).get(field.getFullName()));
				} else if(field.getFullName().equals(ElasticObject.DOC_ID_ALIAS)) {
					data.add(searchHit.id());
				} else if(field.getFullName().equals(ElasticObject.DOC_SCORE)) {
					data.add(searchHit.score());
				} else {
					data.add(null);
				}
			});
			dataRows.add(new DataRow(data));			
		}
		
		fetchedRows += dataRows.size() - (firstPush ? 0 : 1) ;
		state = state == PageDataState.NOT_INITIALIZED ? PageDataState.READY_TO_ITERATE : PageDataState.ITERATION_STARTED;
	}
	
	private void manageCountAll(ResponseBody<?>res) {
		dataRows = new ArrayList<DataRow>();
		List<Object> data = new ArrayList<Object>();
		data.add(res.hits().total().value());
		dataRows.add(new DataRow(data));
		fetchedRows = new Long(dataRows.size());
		state = PageDataState.READY_TO_ITERATE;
	}
	
	private void manageUngroupedExpression(ResponseBody<?> searchResponse) {
		dataRows = new ArrayList<DataRow>();
		
		DataRow dataRow = new DataRow(req.getSelect().getQueryColumns().size());
		
		searchResponse.aggregations().forEach((name, aggregation) -> {
			dataRow.put(Integer.parseInt(name), resolveAggregationValue(aggregation));
		});
		dataRows.add(dataRow);
		
		fetchedRows = new Long(dataRows.size());
		state = PageDataState.READY_TO_ITERATE;
	}
	

	private void manageGroupBy(ResponseBody<?> searchResponse) {
		dataRows = new ArrayList<DataRow>();
		
		Map<String, Aggregate> aggregations = searchResponse.aggregations();
		Map<Integer, Object> rowValues = new HashMap<Integer, Object>();
		
		aggregations.forEach((name, aggregation) -> {
		  exploreGroupByResult(name, aggregation, rowValues);
		});
		fetchedRows = new Long(dataRows.size());
		state = PageDataState.READY_TO_ITERATE;
	}
	
	private boolean isStringAnInteger(String str) {
		try {
			Integer.parseInt(str);
			return true;
		} catch(Exception e) {
			return false;
		}
	}
	
	private void exploreGroupByResult(String name, Aggregate aggregation, Map<Integer, Object> rowValues) {
	    MultiBucketAggregateBase<?> sterms = (MultiBucketAggregateBase<?>)aggregation._get();
		List<?> buckets = sterms.buckets().array();
		for(Object bucket : buckets) {
		    Object key = null;
		    long docCount = 0;
		    Map<String, Aggregate> aggregations = null;
		    if (bucket instanceof StringTermsBucket) {
		      key = ((StringTermsBucket)bucket).key()._get();
		      aggregations = ((StringTermsBucket)bucket).aggregations();
		      docCount = ((StringTermsBucket)bucket).docCount();
		    } else if (bucket instanceof LongTermsBucket) {
		      key = ((LongTermsBucket)bucket).key();
              aggregations = ((LongTermsBucket)bucket).aggregations();
              docCount = ((LongTermsBucket)bucket).docCount();
		    } else if (bucket instanceof DoubleTermsBucket) {
              key = ((DoubleTermsBucket)bucket).key();
              aggregations = ((DoubleTermsBucket)bucket).aggregations();
              docCount = ((DoubleTermsBucket)bucket).docCount();
            }
			if(isStringAnInteger(name)) {
				rowValues.put(Integer.parseInt(name), key);
			}
			if(aggregations != null && aggregations.size() > 0 && aggregations.values().stream().allMatch(p -> p._get() instanceof MultiBucketAggregateBase)) {
			    aggregations.forEach((name1, aggs) -> {
			      exploreGroupByResult(name1, aggs, rowValues);
			    });
			    continue;
			} 
			
			DataRow dataRow = new DataRow(req.getSelect().getQueryColumns().size());
			rowValues.forEach((idx, value) -> dataRow.put(idx, parseValue(idx, value)));
			
			if(aggregations != null) {
			  aggregations.forEach((name1, nestedAggregation) -> {
				dataRow.put(Integer.parseInt(name1), resolveAggregationValue(nestedAggregation));
			  });
			}
			
			for(int idx = 0; idx < req.getSelect().getQueryColumns().size(); idx++) {
				QueryColumn column = req.getSelect().getQueryColumns().get(idx);
				if(column.getAggregatingFunctionExpression() != null && column.getAggregatingFunctionExpression().isAllColumns()) {
					dataRow.put(idx, docCount);
				}
			}
			dataRows.add(dataRow);
		}
	}
	
	@SuppressWarnings("incomplete-switch")
	private Object parseValue(Integer idx, Object value) {
		switch(req.getFieldTypes().get(idx)) {
			case BOOLEAN:
				return (Long)value == 0 ? false: true;
		}
		return value;
	}

	private Object resolveAggregationValue(Aggregate aggregation) {
		if(aggregation.isSimpleValue()) {
			return aggregation.simpleValue().value();
		} else if(aggregation.isValueCount()) {
			return new Double(aggregation.valueCount().value()).longValue();
		} else if(aggregation.isCardinality()) {
			return aggregation.cardinality().value();
		} else if (aggregation.isAvg()) {
		    return aggregation.avg().value();
		} else if (aggregation.isSum()) {
          return aggregation.sum().value();
        } else if (aggregation.isMax()) {
          return aggregation.max().value();
        } else if (aggregation.isMin()) {
          return aggregation.min().value();
        }
		return null;
	}

	private Object resolveField(ElasticObject elObject, Object value) {
		value = resolveType(elObject, value);
		
		if(elObject.getLinkedQueryColumn() != null) {
			if(elObject.getLinkedQueryColumn().getFormatter() != null) {
				return elObject.getLinkedQueryColumn().getFormatter().resolveValue(value);
			}
		}
		
		return value;
	}

	private Object resolveType(ElasticObject elObject, Object value) {
		switch(elObject.getType()) {
			case BOOLEAN:
				if(value != null) {
					return value;
				}
				return null;
			case GEO_POINT:
				return resolveGeoPoint(value);
			case DATE:
			case DATE_NANOS:
				try {
					return sdfTimestamp.parse((String)value).toInstant().atZone(ZoneId.systemDefault()).toLocalDateTime();
				} catch (ParseException e) {
					// log error
					return null;
				}
			default:
				return value;
		}
	}

	private Object resolveGeoPoint(Object value) {
		if(value == null) {
			return null;
		}
		
		String[] gp = ((String)value).split(",");
		return new EsGeoPoint(Double.parseDouble(gp[0]), Double.parseDouble(gp[1]));
		
	}

	public DataRow getCurrentRow() throws SQLException {
		switch (getState()) {
			case NOT_INITIALIZED:
				throw new SQLException("PageData not initialized");
			case READY_TO_ITERATE:
				throw new SQLException("PageData not started");
			default:
				return dataRows.get(currentIdxCurrentRow);
		}
	}

	public boolean isReadyOrStarted() {
		switch (getState()) {
			case READY_TO_ITERATE:
			case ITERATION_STARTED:
				return true;
			default:
				return false;
		}
	}

	public boolean isProvidingData() {
		switch (getState()) {
			case NOT_INITIALIZED:
			case READY_TO_ITERATE:
				return false;
			default:
				return true;
		}
	}

	public boolean isBeforeFirst() {
		return state == PageDataState.NOT_INITIALIZED || state == PageDataState.READY_TO_ITERATE;
	}

	public boolean isFirst() {
		if (isReadyOrStarted()) {
			return currentIdxCurrentRow == 0;
		}
		return false;
	}

	public Object getColumnValue(String columnName) throws SQLException {
		return getCurrentRow().data.get(req.getFieldNames().indexOf(columnName));
	}

	@SuppressWarnings("unchecked")
	public <T> T getColumnValue(String columnName, Class<T> clazz) throws SQLException { // todo: convert type if required													// required
		return (T) getCurrentRow().data.get(req.getFieldNames().indexOf(columnName));
	}

	public Object getColumnValue(int columnIndex) throws SQLException {
		return getCurrentRow().data.get(columnIndex);
	}

	@SuppressWarnings("unchecked")
	public <T> T getColumnValue(int columnIndex, Class<T> clazz) throws SQLException { // todo: convert type if required
		return (T) getCurrentRow().data.get(columnIndex);
	}
	public void setColumnValue(String columnName, Object data) throws SQLException {
		
		getCurrentRow().put(req.getFieldNames().indexOf(columnName), data);
	}

	public PageDataState next() throws SQLException {
		switch (getState()) {
			case NOT_INITIALIZED:
				throw new SQLException("PageData not initialized");
			case READY_TO_ITERATE:
			case ITERATION_STARTED:
				this.state = doNext();
				return this.state;
			default:
				return getState();
		}
	}

	public void clear() {
		dataRows = new ArrayList<DataRow>();
		currentIdxCurrentRow = 0;
		state = PageDataState.NOT_INITIALIZED;
	}

	public void reset() throws SQLException {
		if (iterationStep > 0) {
			moveToRow(0);
			state = PageDataState.READY_TO_ITERATE;
		} else {
			moveToRow(dataRows.size() - 1);
		}
	}

	public void moveToLast() throws SQLException {
		if (iterationStep > 0) {
			moveToRow(dataRows.size() - 1);
		} else {
			moveToRow(0);
			state = PageDataState.READY_TO_ITERATE;
		}
	}

	public void moveToRow(int rowIndex) throws SQLException {
		switch (getState()) {
			case NOT_INITIALIZED:
				throw new SQLException("PageData not initialized");
			case READY_TO_ITERATE:
				throw new SQLException("PageData not started");
			default:
				if (rowIndex >= dataRows.size()) {
					throw new SQLException(String.format("Row %d does not exists on resultset", rowIndex));
				}
				currentIdxCurrentRow = rowIndex - 1;
				if (currentIdxCurrentRow == dataRows.size() - 1) {
					state = PageDataState.ITERATION_FINISHED;
				} else {
					state = PageDataState.ITERATION_STARTED;
				}

				break;
		}
	}

	public void moveByDelta(int rows) throws SQLException {
		int newIndex = currentIdxCurrentRow + rows;
		if (newIndex < 0 || newIndex >= dataRows.size()) {
			throw new SQLException(String.format("Row %d is out of current resultset range", newIndex));
		}

		moveToRow(newIndex);
	}

	public void finish() {
		state = PageDataState.ITERATION_FINISHED;
	}

	public int getSize() {
		return dataRows.size();
	}

	public int getCurrentRowIndex() {
		return currentIdxCurrentRow;
	}

	public PageDataState getState() {
		return state;
	}

	public void setIterationStep(int iterationStep) {
		this.iterationStep = iterationStep;
	}

	public int getColumnIndex(String columnLabel) {
		return req.getFieldNames().indexOf(columnLabel);
	}

	private PageDataState doNext() {
		if (dataRows.size() >= currentIdxCurrentRow) {
			currentIdxCurrentRow += iterationStep;
			return currentIdxCurrentRow == dataRows.size() ? PageDataState.ITERATION_FINISHED : PageDataState.ITERATION_STARTED;
		}

		return PageDataState.ITERATION_FINISHED;
	}

	public boolean oneRowLeft() {
		return currentIdxCurrentRow == dataRows.size() - 2;
	}

	public boolean isEmpty() {
		return dataRows.isEmpty();
	}

	public Long getFetchedRows() {
		return fetchedRows;
	}

	

}
