package org.fpasti.jdbc.esqlj.elastic.query.impl.search.clause.utils;

import java.sql.SQLException;
import java.sql.SQLSyntaxErrorException;
import java.time.LocalDateTime;
import java.time.OffsetDateTime;
import java.time.ZoneId;
import java.util.stream.Collectors;

import org.fpasti.jdbc.esqlj.elastic.query.statement.IWhereCondition;
import org.fpasti.jdbc.esqlj.elastic.query.statement.model.ExpressionEnum;
import org.fpasti.jdbc.esqlj.support.DateUtils;
import org.fpasti.jdbc.esqlj.support.EsWrapException;

import net.sf.jsqlparser.expression.DoubleValue;
import net.sf.jsqlparser.expression.Function;
import net.sf.jsqlparser.expression.JdbcParameter;
import net.sf.jsqlparser.expression.LongValue;
import net.sf.jsqlparser.expression.StringValue;
import net.sf.jsqlparser.expression.operators.arithmetic.Addition;
import net.sf.jsqlparser.expression.operators.arithmetic.Division;
import net.sf.jsqlparser.expression.operators.arithmetic.Multiplication;
import net.sf.jsqlparser.expression.operators.arithmetic.Subtraction;
import net.sf.jsqlparser.expression.operators.relational.ExpressionList;
import net.sf.jsqlparser.schema.Column;

/**
* @author  Fabrizio Pasti - fabrizio.pasti@gmail.com
*/

public class ExpressionResolverValue {

	public static Object evaluateValueExpression(Object expression, IWhereCondition select) throws EsWrapException {
		switch(ExpressionEnum.resolveByInstance(expression)) {	
			case DOUBLE_VALUE:
				DoubleValue doubleValue = (DoubleValue)expression;
				return doubleValue.getValue();
			case STRING_VALUE:
				StringValue stringValue = (StringValue)expression;
				return stringValue.getValue();			
			case LONG_VALUE:
				LongValue longValue = (LongValue)expression;
				return longValue.getValue();
			case FUNCTION:
				return resolveFunction((Function)expression, select);
			case ADDITION:
				Addition addition = (Addition)expression;
				return addition(evaluateValueExpression(addition.getLeftExpression(), select), evaluateValueExpression(addition.getRightExpression(), select));
			case SUBTRACTION:
				Subtraction subraction = (Subtraction)expression;
				return subtraction(evaluateValueExpression(subraction.getLeftExpression(), select), evaluateValueExpression(subraction.getRightExpression(), select));
			case DIVISION:
				Division division = (Division)expression;
				return division(evaluateValueExpression(division.getLeftExpression(), select), evaluateValueExpression(division.getRightExpression(), select));
			case MULTIPLICATION:
				Multiplication multiplication = (Multiplication)expression;
				return multiplication(evaluateValueExpression(multiplication.getLeftExpression(), select), evaluateValueExpression(multiplication.getRightExpression(), select));
			case COLUMN:
				Column column = (Column)expression;
				if(column.getColumnName().equalsIgnoreCase("SYSDATE")) {
					Function fSysDate = new Function();
					fSysDate.setName("SYSDATE");
					return resolveFunction((Function)fSysDate, select);
				} else if(column.getColumnName().equalsIgnoreCase("TRUE")) {
					return true;
				} else if(column.getColumnName().equalsIgnoreCase("FALSE")) {
					return false;
				}
			case EXPRESSION_LIST:
				ExpressionList expressionList = (ExpressionList)expression;
				return expressionList.getExpressions().stream().map(valueExpression -> evaluateValueExpression(valueExpression, select)).collect(Collectors.toList());
			case JDBC_PARAMETER:
				if (null == select) {
					throw new EsWrapException(new SQLException(String.format("Unmanaged expression: %s", ExpressionEnum.resolveByInstance(expression).name())));	
				}
				return select.getParameters().get(((JdbcParameter)expression).getIndex() - 1);
			default:
				throw new EsWrapException(new SQLException(String.format("Unmanaged expression: %s", ExpressionEnum.resolveByInstance(expression).name())));
		}
	}
	
	private static Object resolveFunction(Function function, IWhereCondition select) throws EsWrapException {
		ExpressionList parameters = (ExpressionList)function.getParameters();
		
		switch(function.getName().toUpperCase()) {
			case "TO_DATE":
				if(parameters.getExpressions().size() != 2) {
					throw new EsWrapException(new SQLSyntaxErrorException("TO_DATE with invalid number of parameters"));
				}
				LocalDateTime resolveToDate = DateUtils.resolveToDate((String)evaluateValueExpression(parameters.getExpressions().get(0), select), (String)evaluateValueExpression(parameters.getExpressions().get(1), select));
				return resolveToDate.toInstant(OffsetDateTime.now().getOffset()).toEpochMilli();
			case "NOW":
			case "GETDATE":
			case "CURDATE":
			case "SYSDATE":
				return LocalDateTime.now(ZoneId.systemDefault()).toInstant(OffsetDateTime.now().getOffset()).toEpochMilli();
			case "TRUNC":
				if(parameters.getExpressions().get(0) instanceof Column && ((Column)parameters.getExpressions().get(0)).getColumnName().equalsIgnoreCase("SYSDATE")) {
					return LocalDateTime.now(ZoneId.systemDefault()).toInstant(OffsetDateTime.now().getOffset()).toEpochMilli();
				} else if(parameters.getExpressions().get(0) instanceof Function) {
					Function nestedFunction = (Function)parameters.getExpressions().get(0);
					if(nestedFunction.getName().equalsIgnoreCase("SYSDATE")  
							|| nestedFunction.getName().equalsIgnoreCase("GETDATE")
							|| nestedFunction.getName().equalsIgnoreCase("CURDATE")
							|| nestedFunction.getName().equalsIgnoreCase("NOW")) {
						return LocalDateTime.now(ZoneId.systemDefault()).toInstant(OffsetDateTime.now().getOffset()).toEpochMilli();
					}
				}
				throw new EsWrapException(new SQLSyntaxErrorException(String.format("'%s' unsupported", function.toString())));
		}
		throw new EsWrapException(new SQLSyntaxErrorException(String.format("Function '%s' unsupported", function.getName())));
	}
	
	private static Object addition(Object a, Object b) {
		if(a instanceof Long && b instanceof Long) {
			return (Long)a + (Long)b;
		}

		if(a instanceof Long && b instanceof Double) {
			return (Long)a + (Double)b;
		}
		
		if(a instanceof Double && b instanceof Long) {
			return (Double)a + (Long)b;
		}

		if(a instanceof Double && b instanceof Double) {
			return (Double)a + (Double)b;
		}

		return null;
	}

	private static Object subtraction(Object a, Object b) {
		if(a instanceof Long && b instanceof Long) {
			return (Long)a - (Long)b;
		}

		if(a instanceof Long && b instanceof Double) {
			return (Long)a - (Double)b;
		}
		
		if(a instanceof Double && b instanceof Long) {
			return (Double)a - (Long)b;
		}

		if(a instanceof Double && b instanceof Double) {
			return (Double)a - (Double)b;
		}

		return null;
	}

	private static Object division(Object a, Object b) {
		if(a instanceof Long && b instanceof Long) {
			return (Long)a / (Long)b;
		}

		if(a instanceof Long && b instanceof Double) {
			return (Long)a / (Double)b;
		}
		
		if(a instanceof Double && b instanceof Long) {
			return (Double)a / (Long)b;
		}

		if(a instanceof Double && b instanceof Double) {
			return (Double)a / (Double)b;
		}

		return null;
	}
	
	private static Object multiplication(Object a, Object b) {
		if(a instanceof Long && b instanceof Long) {
			return (Long)a * (Long)b;
		}

		if(a instanceof Long && b instanceof Double) {
			return (Long)a * (Double)b;
		}
		
		if(a instanceof Double && b instanceof Long) {
			return (Double)a * (Long)b;
		}

		if(a instanceof Double && b instanceof Double) {
			return (Double)a * (Double)b;
		}

		return null;
	}

}
