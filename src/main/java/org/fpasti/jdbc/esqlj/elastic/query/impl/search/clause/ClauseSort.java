package org.fpasti.jdbc.esqlj.elastic.query.impl.search.clause;

import org.fpasti.jdbc.esqlj.elastic.query.impl.search.RequestInstance;
import org.fpasti.jdbc.esqlj.elastic.query.statement.SqlStatementSelect;

import co.elastic.clients.elasticsearch._types.FieldSort;
import co.elastic.clients.elasticsearch._types.SortOptions;
import co.elastic.clients.elasticsearch._types.SortOrder;
import net.sf.jsqlparser.schema.Column;

/**
* @author  Fabrizio Pasti - fabrizio.pasti@gmail.com
*/

public class ClauseSort {
	
	public static void manageSort(SqlStatementSelect select, RequestInstance req) {
		select.getOrderByElements().stream().forEach(elem -> {
			req.getSearchSourceBuilder().sort(new SortOptions.Builder()
					.field(new FieldSort.Builder().field(((Column)elem.getExpression()).getColumnName())
							.order(elem.isAsc() ? SortOrder.Asc : SortOrder.Desc).build()).build());
		});
	}

}
