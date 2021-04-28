package org.fpasti.jdbc.esqlj.elastic.query.impl.search;

import java.sql.SQLException;
import java.sql.SQLNonTransientConnectionException;
import org.fpasti.jdbc.esqlj.Configuration;
import org.fpasti.jdbc.esqlj.ConfigurationPropertyEnum;
import org.fpasti.jdbc.esqlj.EsConnection;
import org.fpasti.jdbc.esqlj.elastic.query.impl.search.clause.ClauseSort;
import org.fpasti.jdbc.esqlj.elastic.query.impl.search.clause.ClauseWhere;
import org.fpasti.jdbc.esqlj.elastic.query.impl.search.clause.select.ClauseSelect;
import org.fpasti.jdbc.esqlj.elastic.query.statement.SqlStatementSelect;
import org.fpasti.jdbc.esqlj.elastic.query.statement.model.QueryType;
import org.fpasti.jdbc.esqlj.support.ElasticUtils;
import co.elastic.clients.elasticsearch._types.Time;
import co.elastic.clients.elasticsearch.core.search.PointInTimeReference;

/**
* @author  Fabrizio Pasti - fabrizio.pasti@gmail.com
*/

public class RequestBuilder {

	public static RequestInstance buildRequest(EsConnection connection, SqlStatementSelect select, int fetchSize) throws SQLException {
		RequestInstance req = new RequestInstance(connection, fetchSize, select);
		
		ClauseSelect.manageFields(select, req);
		ClauseSort.manageSort(select, req);
		ClauseWhere.manageWhere(select, req);
		
		build(connection, req, select);
		
		return req;
	}

	private static void build(EsConnection connection, RequestInstance req, SqlStatementSelect select) throws SQLNonTransientConnectionException {
		if(select.getQueryType().equals(QueryType.DOCS)) {
			int size = select.getLimit() != null ? (select.getLimit() > req.getFetchSize() ? req.getFetchSize() : select.getLimit().intValue()) : req.getFetchSize();
			req.getSearchSourceBuilder().size(size);
		}
		
		Long configuration = Configuration.getConfiguration(ConfigurationPropertyEnum.CFG_QUERY_SCROLL_TIMEOUT_MINUTES, Long.class);
		Time time = new Time.Builder().time(String.format("%dm", configuration)).build();
		switch(req.getPaginationMode()) {
			case SCROLL_API:
			    req.getSearchSourceBuilder().scroll(time);
				break;
			case BY_ORDER_WITH_PIT:
				req.getSearchSourceBuilder().maxConcurrentShardRequests(6L); // enable work around
			  
                PointInTimeReference pit = new PointInTimeReference.Builder()
                    .id(ElasticUtils.getPointInTime(connection, req))
                    .keepAlive(time)
                    .build();
                req.getSearchSourceBuilder().pit(pit);
				break;
			default:
		}
	}
	
}
