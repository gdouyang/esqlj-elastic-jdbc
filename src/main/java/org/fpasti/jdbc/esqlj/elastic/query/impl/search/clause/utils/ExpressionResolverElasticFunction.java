package org.fpasti.jdbc.esqlj.elastic.query.impl.search.clause.utils;

import java.sql.SQLSyntaxErrorException;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import org.fpasti.jdbc.esqlj.elastic.query.impl.search.model.EvaluateQueryResult;
import org.fpasti.jdbc.esqlj.support.EsqljConstants;
import org.fpasti.jdbc.esqlj.support.Utils;

import co.elastic.clients.elasticsearch._types.GeoBounds;
import co.elastic.clients.elasticsearch._types.GeoLocation;
import co.elastic.clients.elasticsearch._types.TopLeftBottomRightGeoBounds;
import co.elastic.clients.elasticsearch._types.query_dsl.GeoBoundingBoxQuery;
import co.elastic.clients.elasticsearch._types.query_dsl.QueryBuilders;
import co.elastic.clients.elasticsearch._types.query_dsl.QueryStringQuery.Builder;
import net.sf.jsqlparser.expression.Function;

/**
* @author  Fabrizio Pasti - fabrizio.pasti@gmail.com
*/

public class ExpressionResolverElasticFunction {

	public static EvaluateQueryResult manageExpression(Function function) throws SQLSyntaxErrorException {
		String queryType = function.getName().toUpperCase();
		List<Object> arguments = function.getParameters().getExpressions().stream().map(param -> ExpressionResolverValue.evaluateValueExpression(param, null)).collect(Collectors.toList());
		
		switch(queryType) {
			case "QUERY_STRING":
				return queryString(queryType, arguments);
			case "GEO_BOUNDING_BOX":
				return geoBoundingBox(queryType, arguments);
			default:
				throw new SQLSyntaxErrorException(String.format("Unsupported function: '%s'", function.toString()));
		}
	}

	private static EvaluateQueryResult queryString(String queryType, List<Object> arguments) throws SQLSyntaxErrorException {
		checkMinimumNumberOfParameters(queryType, arguments, 2);
		String query = arguments.get(0).toString();
		String[] fields = arguments.get(1).toString().split(",");
		
		Builder qsqb = QueryBuilders.queryString().query(query);
		qsqb.fields(Arrays.asList(fields));
		
		arguments.stream().skip(2).forEach(param -> {
			Utils.setAttributeInElasticObject(qsqb, getParameterName(param.toString()), getParameterValue(param.toString()));
		});
	
		return new EvaluateQueryResult(qsqb.build()._toQuery());
	}
	
	private static EvaluateQueryResult geoBoundingBox(String queryType, List<Object> arguments) throws SQLSyntaxErrorException {
		checkExactNumberOfParameters(queryType, arguments, 5);
		
		GeoBoundingBoxQuery.Builder builder = QueryBuilders.geoBoundingBox();//.boundingBox(arguments.get(0).toString());
		builder
		  .field("geoPointField")
//		  .validationMethod(GeoValidationMethod.Strict)
//		  .ignoreUnmapped(true)
//		  .boost(1.0f)
		  .boundingBox(new GeoBounds.Builder().tlbr(new TopLeftBottomRightGeoBounds.Builder()
		      .topLeft(new GeoLocation.Builder().coords(Arrays.asList(convertToDouble(arguments.get(2)), convertToDouble(arguments.get(1)))).build())
		      .bottomRight(new GeoLocation.Builder().coords(Arrays.asList(convertToDouble(arguments.get(3)), convertToDouble(arguments.get(3)))).build())
		      .build())
		      .build());
		return new EvaluateQueryResult(builder.build()._toQuery());
	}
	
	private static double convertToDouble(Object value) {
		if(value instanceof Double) {
			return (double)value;
		}
		
		if(value instanceof Integer) {
			return new Double((int)value);
		}
		
		return new Double((long)value);
	}
	
	private static String getParameterName(String param) {
		return param.split(":")[0];
	}

	private static String getParameterValue(String param) {
		return param.split(":")[1];
	}

	private static void checkExactNumberOfParameters(String queryType, List<Object> arguments, int numMinNumOfArguments) throws SQLSyntaxErrorException {
		if(arguments.size() != numMinNumOfArguments) {
			throw new SQLSyntaxErrorException(String.format("%s ::%s requires %d parameters", EsqljConstants.ESQLJ_WHERE_CLAUSE, queryType, numMinNumOfArguments));
		}
	}
	
	private static void checkMinimumNumberOfParameters(String queryType, List<Object> arguments, int numMinNumOfArguments) throws SQLSyntaxErrorException {
		if(arguments.size() < numMinNumOfArguments) {
			throw new SQLSyntaxErrorException(String.format("%s ::%s requires at least %d parameters", EsqljConstants.ESQLJ_WHERE_CLAUSE, queryType, numMinNumOfArguments));
		}
	}
}
