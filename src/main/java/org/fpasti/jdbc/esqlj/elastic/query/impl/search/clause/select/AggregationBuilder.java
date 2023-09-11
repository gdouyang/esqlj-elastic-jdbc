package org.fpasti.jdbc.esqlj.elastic.query.impl.search.clause.select;

import java.sql.SQLSyntaxErrorException;
import org.fpasti.jdbc.esqlj.Configuration;
import org.fpasti.jdbc.esqlj.ConfigurationPropertyEnum;
import org.fpasti.jdbc.esqlj.elastic.query.impl.search.RequestInstance;
import org.fpasti.jdbc.esqlj.elastic.query.impl.search.clause.ClauseHaving;
import org.fpasti.jdbc.esqlj.elastic.query.statement.SqlStatementSelect;
import org.fpasti.jdbc.esqlj.elastic.query.statement.model.QueryColumn;
import co.elastic.clients.elasticsearch._types.InlineScript;
import co.elastic.clients.elasticsearch._types.Script;
import co.elastic.clients.elasticsearch._types.SortOrder;
import co.elastic.clients.elasticsearch._types.aggregations.Aggregation;
import co.elastic.clients.elasticsearch._types.aggregations.AverageAggregation;
import co.elastic.clients.elasticsearch._types.aggregations.CardinalityAggregation;
import co.elastic.clients.elasticsearch._types.aggregations.MaxAggregation;
import co.elastic.clients.elasticsearch._types.aggregations.MinAggregation;
import co.elastic.clients.elasticsearch._types.aggregations.SumAggregation;
import co.elastic.clients.elasticsearch._types.aggregations.TermsAggregation;
import co.elastic.clients.elasticsearch._types.aggregations.ValueCountAggregation;
import co.elastic.clients.elasticsearch.core.SearchRequest;
import co.elastic.clients.elasticsearch.core.SearchRequest.Builder;
import co.elastic.clients.util.NamedValue;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.statement.select.OrderByElement;

/**
* @author  Fabrizio Pasti - fabrizio.pasti@gmail.com
*/

public class AggregationBuilder {

	@SuppressWarnings("incomplete-switch")
	public static void doAggregation(RequestInstance req) throws SQLSyntaxErrorException {
		switch(req.getSelect().getQueryType()) {
			case AGGR_UNGROUPED_EXPRESSIONS:
				manageUngroupedExpression(req);
				break;
			case AGGR_GROUP_BY:
				manageGroupByExpressions(req);
				break;	
			case DISTINCT_DOCS:
				manageDistinctColumns(req);
				break;
		}
		req.getSearchSourceBuilder().size(0);
	}

	private static void manageUngroupedExpression(RequestInstance req) throws SQLSyntaxErrorException {
		Builder builder = req.getSearchSourceBuilder();
		for(Integer idx = 0; idx < req.getSelect().getQueryColumns().size(); idx++) {
			QueryColumn queryColumn = req.getSelect().getQueryColumns().get(idx);
			addGroupingFunction(queryColumn, idx.toString(), req.getSelect(), null, builder);
		}
	}

	public static void manageGroupByExpressions(RequestInstance req) throws SQLSyntaxErrorException {
		Builder builder = req.getSearchSourceBuilder();
		TermsAggregationBuilderWrapper firstTermsAggregationBuilder = null;
		TermsAggregationBuilderWrapper currentTermsAggregationBuilder = null;
		String firstPosition = null;
		for(String column : req.getSelect().getGroupByColumns()) {
		    String positionInClauseSelect = getPositionInClauseSelect(req.getSelect(), column);
		    TermsAggregation.Builder tab = new TermsAggregation.Builder()
              .field(column)
              .size(Configuration.getConfiguration(ConfigurationPropertyEnum.CFG_MAX_GROUP_BY_RETRIEVED_ELEMENTS, Integer.class));
			
			manageOrdering(column, tab, req.getSelect());
			
			TermsAggregationBuilderWrapper b = new TermsAggregationBuilderWrapper(tab);
			if(firstTermsAggregationBuilder == null) {
			  firstTermsAggregationBuilder = b;
              firstPosition = positionInClauseSelect;
            } else {
              currentTermsAggregationBuilder.aggregations(positionInClauseSelect, tab);
            }
			currentTermsAggregationBuilder = b;
		}
		
		final TermsAggregationBuilderWrapper deeperTermsAggregationBuilder = currentTermsAggregationBuilder;
		for(Integer i = 0; i < req.getSelect().getQueryColumns().size(); i++) {
			QueryColumn column = req.getSelect().getQueryColumns().get(i);
			if(column.getAggregatingFunctionExpression() != null) {
				addGroupingFunction(column, i.toString(), req.getSelect(), deeperTermsAggregationBuilder, builder);
			}
		}
		
		if(req.getSelect().getHavingCondition() != null) {
			ClauseHaving.manageHavingCondition(req.getSelect(), deeperTermsAggregationBuilder);
		}
		builder.aggregations(firstPosition, currentTermsAggregationBuilder.build());
	}
	
	private static void manageDistinctColumns(RequestInstance req) {
	    SearchRequest.Builder builder = req.getSearchSourceBuilder();
	    
	    TermsAggregationBuilderWrapper firstTermsAggregationBuilder = null;
	    TermsAggregationBuilderWrapper currentTermsAggregationBuilder = null;
		for(QueryColumn queryColumn : req.getSelect().getQueryColumns()) {
		  String positionInClauseSelect = getPositionInClauseSelect(req.getSelect(), queryColumn.getName());
		  TermsAggregation.Builder tab = new TermsAggregation.Builder()
		      .field(queryColumn.getName())
		      .size(Configuration.getConfiguration(ConfigurationPropertyEnum.CFG_MAX_GROUP_BY_RETRIEVED_ELEMENTS, Integer.class));
			
			manageOrdering(queryColumn.getName(), tab, req.getSelect());
			
			TermsAggregationBuilderWrapper b = new TermsAggregationBuilderWrapper(tab);
			if(firstTermsAggregationBuilder == null) {
			  firstTermsAggregationBuilder = b;
			} else {
			    currentTermsAggregationBuilder.aggregations(positionInClauseSelect, tab);
			}
			currentTermsAggregationBuilder = b;
		}
		builder.aggregations("0", firstTermsAggregationBuilder.build());
	}

	private static void manageOrdering(String columnName, TermsAggregation.Builder termsAggregation, SqlStatementSelect select) {
		OrderByElement orderByElement = select.getOrderByElements().stream().filter(orderBy -> ((Column)orderBy.getExpression()).getColumnName().equalsIgnoreCase(columnName)).findFirst().orElse(null);
		if(orderByElement != null) {
		    NamedValue<SortOrder> of = NamedValue.of("_key", orderByElement.isAsc() ? SortOrder.Asc : SortOrder.Desc);
			termsAggregation.order(of);
		}
	}

	private static void addGroupingFunction(QueryColumn column, String columnPosition, SqlStatementSelect select, TermsAggregationBuilderWrapper termsAggregation, SearchRequest.Builder builder) throws SQLSyntaxErrorException {
		Aggregation aggregationBuilder = null;
		
		switch(column.getFunctionType()) {
			case AVG:
				aggregationBuilder = AverageAggregation.of(b -> {
					b.field(stripDoubleQuotes(column.getAggregatingFunctionExpression().getParameters().getExpressions().get(0).toString()));
					return b;
				})._toAggregation();
				break;
			case COUNT:
				if(column.getAggregatingFunctionExpression().isAllColumns()) {
					aggregationBuilder = ValueCountAggregation.of(b -> {
						b.script(new Script.Builder().inline(new InlineScript.Builder().source("1").build()).build());
						return b;
					})._toAggregation();
				} else if(column.getAggregatingFunctionExpression().isDistinct()) {
					aggregationBuilder = CardinalityAggregation.of(b -> {
						b.field(stripDoubleQuotes(column.getAggregatingFunctionExpression().getParameters().getExpressions().get(0).toString()));
						return b;
					})._toAggregation();
				} else {
					aggregationBuilder = ValueCountAggregation.of(b -> {
						b.field(stripDoubleQuotes(column.getAggregatingFunctionExpression().getParameters().getExpressions().get(0).toString()));
						return b;
					})._toAggregation();
				}
				break;
			case MAX:
				aggregationBuilder = MaxAggregation.of(b -> {
					b.field(stripDoubleQuotes(column.getAggregatingFunctionExpression().getParameters().getExpressions().get(0).toString()));
					return b;
				})._toAggregation();
				break;
			case MIN:
				aggregationBuilder = MinAggregation.of(b -> {
					b.field(stripDoubleQuotes(column.getAggregatingFunctionExpression().getParameters().getExpressions().get(0).toString()));
					return b;
				})._toAggregation();
				break;
			case SUM:
				aggregationBuilder = SumAggregation.of(b -> {
					b.field(stripDoubleQuotes(column.getAggregatingFunctionExpression().getParameters().getExpressions().get(0).toString()));
					return b;
				})._toAggregation();
				break;
			default:
				throw new SQLSyntaxErrorException(String.format("Expression %s unsupported", column.getAggregatingFunctionExpression().getName()));
		}
		
		if(aggregationBuilder != null) {
			if(termsAggregation != null) {
				termsAggregation.aggregations(columnPosition, aggregationBuilder);
				
				OrderByElement orderByElement = select.getOrderByElements().stream().filter(orderBy -> ((Column)orderBy.getExpression()).getColumnName().equalsIgnoreCase(column.getName()) || ((Column)orderBy.getExpression()).getColumnName().equalsIgnoreCase(column.getAlias())).findFirst().orElse(null);
				if(orderByElement != null) {
					NamedValue<SortOrder> of = NamedValue.of(columnPosition, orderByElement.isAsc() ? SortOrder.Asc : SortOrder.Desc);
		            termsAggregation.order(of);
				}
			} else {
			    builder.aggregations(columnPosition, aggregationBuilder);
			}
		}
	}

	
	
	private static String stripDoubleQuotes(String str) {
		return str.replaceAll("\"", "");
	}

	private static String getPositionInClauseSelect(SqlStatementSelect select, String columnName) {
		for(Integer i = 0; i < select.getQueryColumns().size(); i++) {
			if(select.getQueryColumns().get(i).getName().equalsIgnoreCase(columnName)) {
				return i.toString(); 
			}
		}
		return columnName;
	}
}
