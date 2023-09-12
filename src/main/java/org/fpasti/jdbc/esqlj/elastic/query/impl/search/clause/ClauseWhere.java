	package org.fpasti.jdbc.esqlj.elastic.query.impl.search.clause;


import java.sql.SQLSyntaxErrorException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.fpasti.jdbc.esqlj.elastic.query.impl.search.RequestInstance;
import org.fpasti.jdbc.esqlj.elastic.query.impl.search.clause.utils.ExpressionResolverElasticFunction;
import org.fpasti.jdbc.esqlj.elastic.query.impl.search.clause.utils.ExpressionResolverValue;
import org.fpasti.jdbc.esqlj.elastic.query.impl.search.model.ElasticScriptMethodEnum;
import org.fpasti.jdbc.esqlj.elastic.query.impl.search.model.EvaluateQueryResult;
import org.fpasti.jdbc.esqlj.elastic.query.impl.search.model.TermsQuery;
import org.fpasti.jdbc.esqlj.elastic.query.statement.IWhereCondition;
import org.fpasti.jdbc.esqlj.elastic.query.statement.SqlStatementSelect;
import org.fpasti.jdbc.esqlj.elastic.query.statement.model.ExpressionEnum;
import org.fpasti.jdbc.esqlj.elastic.query.statement.model.QueryColumn;

import co.elastic.clients.elasticsearch._types.FieldValue;
import co.elastic.clients.elasticsearch._types.InlineScript;
import co.elastic.clients.elasticsearch._types.Script;
import co.elastic.clients.elasticsearch._types.query_dsl.BoolQuery;
import co.elastic.clients.elasticsearch._types.query_dsl.Query;
import co.elastic.clients.elasticsearch._types.query_dsl.QueryBuilders;
import co.elastic.clients.elasticsearch._types.query_dsl.TermQuery;
import co.elastic.clients.elasticsearch._types.query_dsl.TermsQueryField;
import co.elastic.clients.json.JsonData;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.ExtractExpression;
import net.sf.jsqlparser.expression.Function;
import net.sf.jsqlparser.expression.NotExpression;
import net.sf.jsqlparser.expression.Parenthesis;
import net.sf.jsqlparser.expression.operators.conditional.AndExpression;
import net.sf.jsqlparser.expression.operators.conditional.OrExpression;
import net.sf.jsqlparser.expression.operators.relational.Between;
import net.sf.jsqlparser.expression.operators.relational.EqualsTo;
import net.sf.jsqlparser.expression.operators.relational.ExpressionList;
import net.sf.jsqlparser.expression.operators.relational.GreaterThan;
import net.sf.jsqlparser.expression.operators.relational.GreaterThanEquals;
import net.sf.jsqlparser.expression.operators.relational.InExpression;
import net.sf.jsqlparser.expression.operators.relational.IsNullExpression;
import net.sf.jsqlparser.expression.operators.relational.LikeExpression;
import net.sf.jsqlparser.expression.operators.relational.MinorThan;
import net.sf.jsqlparser.expression.operators.relational.MinorThanEquals;
import net.sf.jsqlparser.expression.operators.relational.NotEqualsTo;
import net.sf.jsqlparser.schema.Column;

/**
* @author  Fabrizio Pasti - fabrizio.pasti@gmail.com
*/

public class ClauseWhere {
	
	public static void manageWhere(SqlStatementSelect select, RequestInstance req) throws SQLSyntaxErrorException {
		if(select.getWhereCondition() == null) {
			return;
		}
		
		EvaluateQueryResult result = evaluateWhereExpression(select.getWhereCondition(), select);
		
		Query qb = null;
		
		switch(result.getType()) {
			case ONLY_ONE:
				qb = result.getQueryBuilders().get(0);
				break;
			case ONLY_ONE_NOT:
				qb = result.getNotQueryBuilders().get(0);
				break;
			case ONLY_ONE_TERMS:
				Map.Entry<String,List<Object>> terms = result.getTermsQuery().getEqualObjects().entrySet().iterator().next();
				if(terms.getValue().size() == 1) {
					qb = QueryBuilders.term().field(terms.getKey()).value(FieldValue.of(JsonData.of(terms.getValue().get(0)))).build()._toQuery();
				} else {
					List<FieldValue> v = terms.getValue().stream().map(o -> new FieldValue.Builder().anyValue(JsonData.of(o)).build()).collect(Collectors.toList());
					TermsQueryField build = new TermsQueryField.Builder().value(v).build();
					qb = QueryBuilders.terms().field(terms.getKey()).terms(build).build()._toQuery();
				}
				break;
			case ONLY_ONE_NOT_TERMS:
				Map.Entry<String,List<Object>> notTerms = result.getTermsQuery().getNotEqualObjects().entrySet().iterator().next();
				TermQuery query = TermQuery.of((q) -> {
					q.field(notTerms.getKey());
					q.value(notTerms.getValue().get(0).toString());
					return q;
				});
				qb = QueryBuilders.bool().mustNot(query._toQuery()).build()._toQuery();
				break;
			case MIXED:
				BoolQuery.Builder bool = QueryBuilders.bool();
				getQueryBuilderFromResult(select.getWhereCondition(), result, bool);
				qb = bool.build()._toQuery();
				break;
		}
		
		req.getSearchSourceBuilder().query(qb);
	}
	
	public static Query manageDleteWhere(IWhereCondition select) throws SQLSyntaxErrorException {
      if(select.getWhereCondition() == null) {
          return null;
      }
      
      EvaluateQueryResult result = evaluateWhereExpression(select.getWhereCondition(), select);
      
      Query qb = null;
      
      BoolQuery.Builder bool = QueryBuilders.bool();
      getQueryBuilderFromResult(select.getWhereCondition(), result, bool);
      qb = bool.build()._toQuery();
      return qb;
  }
	
	@SuppressWarnings("unchecked")
	private static EvaluateQueryResult evaluateWhereExpression(Expression expression, IWhereCondition select) throws SQLSyntaxErrorException {
		switch(ExpressionEnum.resolveByInstance(expression)) {
			case AND_EXPRESSION:
				AndExpression andExpression = (AndExpression)expression;
				EvaluateQueryResult resAndLeft = evaluateWhereExpression(andExpression.getLeftExpression(), select);
				EvaluateQueryResult resAndRight = evaluateWhereExpression(andExpression.getRightExpression(), select);
				resAndLeft.merge(true, resAndRight);
				return resAndLeft;
			case OR_EXPRESSION:
				OrExpression orExpression = (OrExpression)expression;
				Query leftBoolQueryBuilder = null;
				Query rightBoolQueryBuilder = null;

				EvaluateQueryResult resOrLeft = evaluateWhereExpression(orExpression.getLeftExpression(), select);
				EvaluateQueryResult resOrRight = evaluateWhereExpression(orExpression.getRightExpression(), select);
				
				if(ExpressionEnum.isInstanceOf(orExpression.getLeftExpression(), ExpressionEnum.AND_EXPRESSION) 
						|| ExpressionEnum.isInstanceOf(orExpression.getLeftExpression(), ExpressionEnum.PARENTHESIS)
						|| ExpressionEnum.isInstanceOf(orExpression.getLeftExpression(), ExpressionEnum.NOT_EXPRESSION)) {
					leftBoolQueryBuilder = createBoolQueryBuilder(resOrLeft);
				}
				
				if(ExpressionEnum.isInstanceOf(orExpression.getRightExpression(), ExpressionEnum.AND_EXPRESSION) 
						|| ExpressionEnum.isInstanceOf(orExpression.getRightExpression(), ExpressionEnum.PARENTHESIS) 
						|| ExpressionEnum.isInstanceOf(orExpression.getRightExpression(), ExpressionEnum.NOT_EXPRESSION)) {
					rightBoolQueryBuilder = createBoolQueryBuilder(resOrRight);
				}
				
				Query qbOr = null;
				if(leftBoolQueryBuilder != null && rightBoolQueryBuilder == null) {
					qbOr = mapResultToQueryBuilder(leftBoolQueryBuilder, resOrRight);
				} else if(rightBoolQueryBuilder != null && leftBoolQueryBuilder == null) {
					qbOr = mapResultToQueryBuilder(rightBoolQueryBuilder, resOrLeft);
				} else if(leftBoolQueryBuilder != null && rightBoolQueryBuilder != null) {
					qbOr = QueryBuilders.bool()
							.should(rightBoolQueryBuilder)
							.should(leftBoolQueryBuilder).build()._toQuery();
				}
				
				if(qbOr != null) {
					return new EvaluateQueryResult(qbOr);
				}
				
				return resOrLeft.merge(false, resOrRight);
			case PARENTHESIS:
				Parenthesis parenthesis = (Parenthesis)expression;
				return evaluateWhereExpression(parenthesis.getExpression(), select);
			case GREATER_THAN:
				GreaterThan greaterThan = (GreaterThan)expression;
				if(greaterThan.getLeftExpression() instanceof ExtractExpression) {
					return resolveExtract(greaterThan.getLeftExpression(), greaterThan.getRightExpression(), ">", select);
				}
				String column = getColumn(greaterThan.getLeftExpression(), select);
				JsonData value = JsonData.of(ExpressionResolverValue.evaluateValueExpression(greaterThan.getRightExpression()));
				return new EvaluateQueryResult(QueryBuilders.range(r -> r.field(column).gt(value)));
			case GREATER_THAN_EQUALS:
				GreaterThanEquals greaterThanEquals = (GreaterThanEquals)expression;
				if(greaterThanEquals.getLeftExpression() instanceof ExtractExpression) {
					return resolveExtract(greaterThanEquals.getLeftExpression(), greaterThanEquals.getRightExpression(), ">=", select);
				}
				String column2 = getColumn(greaterThanEquals.getLeftExpression(), select);
				return new EvaluateQueryResult(QueryBuilders.range(r -> r.field(column2)
						.gte(JsonData.of(ExpressionResolverValue.evaluateValueExpression(greaterThanEquals.getRightExpression())))));
			case MINOR_THAN:
				MinorThan minorThan = (MinorThan)expression;
				if(minorThan.getLeftExpression() instanceof ExtractExpression) {
					return resolveExtract(minorThan.getLeftExpression(), minorThan.getRightExpression(), "<", select);
				}
				String column3 = getColumn(minorThan.getLeftExpression(), select);
				return new EvaluateQueryResult(QueryBuilders.range(r -> r.field(column3)
						.lt(JsonData.of(ExpressionResolverValue.evaluateValueExpression(minorThan.getRightExpression())))));
			case MINOR_THAN_EQUALS:
				MinorThanEquals minorThanEquals = (MinorThanEquals)expression;
				if(minorThanEquals.getLeftExpression() instanceof ExtractExpression) {
					return resolveExtract(minorThanEquals.getLeftExpression(), minorThanEquals.getRightExpression(), "<=", select);
				}
				String column4 = getColumn(minorThanEquals.getLeftExpression(), select);
				return new EvaluateQueryResult(QueryBuilders.range(r -> r.field(column4)
						.lte(JsonData.of(ExpressionResolverValue.evaluateValueExpression(minorThanEquals.getRightExpression())))));
			case EQUALS_TO:
				EqualsTo equalsTo = (EqualsTo)expression;
				if(equalsTo.getLeftExpression() instanceof ExtractExpression) {
					return resolveExtract(equalsTo.getLeftExpression(), equalsTo.getRightExpression(), "==", select);
				}
				EvaluateQueryResult etQr = new EvaluateQueryResult();
				etQr.addEqualTerm(getColumn(equalsTo.getLeftExpression(), select), ExpressionResolverValue.evaluateValueExpression(equalsTo.getRightExpression()));
				return etQr;
			case NOT_EQUALS_TO:
				NotEqualsTo notEqualsTo = (NotEqualsTo)expression;
				if(notEqualsTo.getLeftExpression() instanceof ExtractExpression) {
					return resolveExtract(notEqualsTo.getLeftExpression(), notEqualsTo.getRightExpression(), "!=", select);
				}
				EvaluateQueryResult netQr = new EvaluateQueryResult();
				netQr.addNotEqualTerm(getColumn(notEqualsTo.getLeftExpression(), select), ExpressionResolverValue.evaluateValueExpression(notEqualsTo.getRightExpression()));
				return netQr;
			case IS_NULL_EXPRESSION:
 				IsNullExpression isNullExpression = (IsNullExpression)expression;
 				String column5 = getColumn(isNullExpression.getLeftExpression(), select);
 				if(isNullExpression.isNot()) {
 					return new EvaluateQueryResult(QueryBuilders.exists(e -> {
 						e.field(column5);
 						return e;
 					}));
 				}
				BoolQuery.Builder qbEqrIneNot = QueryBuilders.bool();
				qbEqrIneNot.mustNot(QueryBuilders.exists(e -> e.field(column5)));
				EvaluateQueryResult eqrIne = new EvaluateQueryResult(qbEqrIneNot.build()._toQuery());
				eqrIne.setReverseNegateOnNot(true);
				return eqrIne;
			case NOT_EXPRESSION:
				NotExpression notExpression = (NotExpression)expression;
				EvaluateQueryResult res = evaluateWhereExpression(notExpression.getExpression(), select);
				BoolQuery.Builder qbNot = QueryBuilders.bool();
				getQueryBuilderFromResult(notExpression.getExpression(), res, qbNot);
				BoolQuery.Builder qb = QueryBuilders.bool();
				qb.mustNot(qbNot.build()._toQuery());
				return new EvaluateQueryResult(qb.build()._toQuery());
			case BETWEEN:
				Between between = (Between)expression;
				if(between.getLeftExpression() instanceof ExtractExpression) {
					return resolveExtractBetween(between, select);
				}
				String column6 = getColumn(between.getLeftExpression(), select);
				return new EvaluateQueryResult(QueryBuilders.range(r -> r.field(column6)
						.gte(JsonData.of(ExpressionResolverValue.evaluateValueExpression(between.getBetweenExpressionStart())))
						.lte(JsonData.of(ExpressionResolverValue.evaluateValueExpression(between.getBetweenExpressionEnd())))));
			case LIKE_EXPRESSION:
				LikeExpression likeExpression = (LikeExpression)expression;
				String column7 = getColumn(likeExpression.getLeftExpression(), select);
				return new EvaluateQueryResult(QueryBuilders.wildcard(w -> w.field(column7)
						.value((String)ExpressionResolverValue.evaluateValueExpression(likeExpression.getRightExpression()))));
			case IN_EXPRESSION:
				InExpression inExpression = (InExpression)expression;
				if(inExpression.getLeftExpression() instanceof ExtractExpression) {
					return resolveExtract(inExpression.getLeftExpression(), inExpression.getRightItemsList(), "==", select);
				}
				EvaluateQueryResult etQrIe = new EvaluateQueryResult();
				etQrIe.addEqualTerms(getColumn(inExpression.getLeftExpression(), select), (List<Object>)ExpressionResolverValue.evaluateValueExpression(inExpression.getRightItemsList()));
				return etQrIe;
			case FUNCTION:
				Function function = (Function)expression;
				return ExpressionResolverElasticFunction.manageExpression(function);
			default:
				throw new SQLSyntaxErrorException(String.format("Unmanaged expression: %s", ExpressionEnum.resolveByInstance(expression).name()));
		}
	}

	private static EvaluateQueryResult resolveExtract(Expression extractExpression, Object valueExpression, String operator, IWhereCondition select) throws SQLSyntaxErrorException {
		ExtractExpression extract = (ExtractExpression)extractExpression;
		ElasticScriptMethodEnum scriptDateMethod = null;
		try {
			scriptDateMethod = ElasticScriptMethodEnum.valueOf(extract.getName());
		} catch(IllegalArgumentException e) {
			throw new SQLSyntaxErrorException(String.format("Unsupported extract params '%s'", extract.getName()));
		}

		if(ExpressionEnum.resolveByInstance(valueExpression) == ExpressionEnum.EXPRESSION_LIST) {
			ExpressionList expressionList = (ExpressionList)valueExpression;
			Map<String, JsonData> params = new HashMap<String, JsonData>();
			String scriptExpression = "";
			for(int i = 0; i < expressionList.getExpressions().size(); i++) {
				Expression expression = expressionList.getExpressions().get(i);
				params.put(String.format("param%d", i), JsonData.of(ExpressionResolverValue.evaluateValueExpression(expression)));
				scriptExpression = scriptExpression.concat(scriptExpression.length() == 0 ? "" : " || ").concat(String.format("doc.%s.value.%s %s params.param%d", getColumn(extract.getExpression(), select), scriptDateMethod.getMethod(), operator, i));
			}
			Script script = new Script.Builder().inline(new InlineScript.Builder().params(params)
					.source(scriptExpression).build()).build();
			Query sq = QueryBuilders.script(s -> {
				s.script(script);
				return s;
			});
			return new EvaluateQueryResult(sq);
		}
		
		Map<String, JsonData> params = new HashMap<String, JsonData>();
		params.put("param", JsonData.of(ExpressionResolverValue.evaluateValueExpression(valueExpression)));
		Script script = new Script.Builder().inline(new InlineScript.Builder().params(params)
				.source(String.format("doc.%s.value.%s %s params.param", getColumn(extract.getExpression(), select), scriptDateMethod.getMethod(), operator)).build()).build();
		Query sq = QueryBuilders.script(s -> {
			s.script(script);
			return s;
		});
		return new EvaluateQueryResult(sq);
	}
	
	private static EvaluateQueryResult resolveExtractBetween(Between between, IWhereCondition select) throws SQLSyntaxErrorException {
		ExtractExpression extract = (ExtractExpression)between.getLeftExpression();
		ElasticScriptMethodEnum scriptDateMethod = null;
		try {
			scriptDateMethod = ElasticScriptMethodEnum.valueOf(extract.getName());
		} catch(IllegalArgumentException e) {
			throw new SQLSyntaxErrorException(String.format("Unsupported extract params '%s'", extract.getName()));
		}
		Map<String, JsonData> params = new HashMap<String, JsonData>();
		params.put("param1", JsonData.of(ExpressionResolverValue.evaluateValueExpression(between.getBetweenExpressionStart())));
		params.put("param2", JsonData.of(ExpressionResolverValue.evaluateValueExpression(between.getBetweenExpressionEnd())));
		
		Script script = new Script.Builder().inline(new InlineScript.Builder().params(params)
				.source(String.format("doc.%s.value.%s >= params.param1 && doc.%s.value.%s <= params.param2", getColumn(extract.getExpression(), select), scriptDateMethod.getMethod(), getColumn(extract.getExpression(), select), scriptDateMethod.getMethod())).build()).build();
		Query sq = QueryBuilders.script(s -> {
			s.script(script);
			return s;
		});
		return new EvaluateQueryResult(sq);
	}

	private static void getQueryBuilderFromResult(Expression expression, EvaluateQueryResult result,
			BoolQuery.Builder qb) {
		if(!result.isListEmpty()) {
			if(result.isAnd()) {
				qb.must(result.getQueryBuilders());
			} else {
				qb.should(result.getQueryBuilders());
			}
		}
		if(!result.isNotListEmpty()) {
			qb.mustNot(result.getNotQueryBuilders());
		}
		if(!result.isTermsEmpty()) {
			addTermsQuery(qb, result.getTermsQuery(), result.isAnd());
		}
	}
	
	private static Query createBoolQueryBuilder(EvaluateQueryResult queryResult) {
		
		return BoolQuery.of(b -> {
			b.must(queryResult.getQueryBuilders())
			.mustNot(queryResult.getNotQueryBuilders());
			
			addTermsQuery(b, queryResult.getTermsQuery(), true);
			return b;
		})._toQuery();
	}
	
	private static Query mapResultToQueryBuilder(Query queryBuilder, EvaluateQueryResult result) {
		return BoolQuery.of(b -> {
			b.should(queryBuilder)
			.should(result.getQueryBuilders());
			
			if(result.getNotQueryBuilders().size() > 0) {
				b.should(BoolQuery.of(b1 -> b1.mustNot(result.getNotQueryBuilders()))._toQuery());
			}
			
			addTermsQuery(b, result.getTermsQuery(), false);
			return b;
		})._toQuery();
	}

	private static void addTermsQuery(BoolQuery.Builder qb, TermsQuery queryContents, boolean and) {
		if(queryContents.getEqualObjects() != null && !queryContents.getEqualObjects().isEmpty()) {
			queryContents.getEqualObjects().forEach((field, values) -> {
				if(and) {
					values.stream().forEach(value -> qb.must(TermQuery.of(t -> t.field(field).value(FieldValue.of(JsonData.of(value))))._toQuery()));
				}
				else {
					List<FieldValue> termsValues = values.stream().map(v -> {
						return FieldValue.of(JsonData.of(v));
					}).collect(Collectors.toList());
					Query terms = QueryBuilders.terms(t -> t.field(field).terms(new TermsQueryField.Builder().value(termsValues).build()));
					qb.should(terms);
				} 
			});
		}
		
		if(queryContents.getNotEqualObjects() != null && !queryContents.getNotEqualObjects().isEmpty()) {
			BoolQuery.Builder mustNotQb = QueryBuilders.bool();
			
			queryContents.getNotEqualObjects().forEach((field, values) -> {
				if(and) {
					values.stream().forEach(value -> qb.mustNot(TermQuery.of(t -> t.field(field).value(FieldValue.of(JsonData.of(value))))._toQuery()));
				} else {
					List<FieldValue> termsValues = values.stream().map(v -> {
						return FieldValue.of(JsonData.of(v));
					}).collect(Collectors.toList());
					Query terms = QueryBuilders.terms(t -> t.field(field).terms(new TermsQueryField.Builder().value(termsValues).build()));
					mustNotQb.mustNot(terms);
				} 
			});
			
			if(!and) {
				qb.should(mustNotQb.build()._toQuery());
			}
		}
	}
	
	public static String getColumn(Expression expression, IWhereCondition select) throws SQLSyntaxErrorException {
		if(!(expression instanceof Column)) {
			throw new SQLSyntaxErrorException(String.format("Unsupported WHERE expression: %s", expression.toString()));
		}
		
		Column column = (Column)expression;
		String columnName = column.getColumnName().replace("\"", "");
		QueryColumn columnField = select.getColumnsByNameOrAlias(columnName);
		return columnField != null ? columnField.getName() : columnName;
	}
	
}
