package org.fpasti.jdbc.esqlj.elastic.query.impl.search;

import java.sql.SQLException;
import java.sql.SQLSyntaxErrorException;
import java.time.LocalDateTime;
import java.time.ZoneId;

import org.fpasti.jdbc.esqlj.elastic.query.statement.model.ExpressionEnum;
import org.fpasti.jdbc.esqlj.support.ToDateUtils;

import net.sf.jsqlparser.expression.DoubleValue;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.Function;
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

public class ValueExpressionResolver {

	public static Object evaluateValueExpression(Expression expression) throws SQLException {
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
				return resolveFunction((Function)expression);
			case ADDITION:
				Addition addition = (Addition)expression;
				return addition(evaluateValueExpression(addition.getLeftExpression()), evaluateValueExpression(addition.getRightExpression()));
			case SUBTRACTION:
				Subtraction subraction = (Subtraction)expression;
				return subtraction(evaluateValueExpression(subraction.getLeftExpression()), evaluateValueExpression(subraction.getRightExpression()));
			case DIVISION:
				Division division = (Division)expression;
				return division(evaluateValueExpression(division.getLeftExpression()), evaluateValueExpression(division.getRightExpression()));
			case MULTIPLICATION:
				Multiplication multiplication = (Multiplication)expression;
				return multiplication(evaluateValueExpression(multiplication.getLeftExpression()), evaluateValueExpression(multiplication.getRightExpression()));
			case COLUMN:
				Column column = (Column)expression;
				if(column.getColumnName().equalsIgnoreCase("SYSDATE")) {
					Function fSysDate = new Function();
					fSysDate.setName("SYSDATE");
					return resolveFunction((Function)fSysDate);
				} else if(column.getColumnName().equalsIgnoreCase("TRUE")) {
					return true;
				} else if(column.getColumnName().equalsIgnoreCase("FALSE")) {
					return false;
				}
			default:
				throw new SQLException(String.format("Unmanaged expression: %s", ExpressionEnum.resolveByInstance(expression).name()));
		}
	}
	
	private static Object resolveFunction(Function function) throws SQLException {
		ExpressionList parameters = (ExpressionList)function.getParameters();
		
		switch(function.getName().toUpperCase()) {
			case "TO_DATE":
				if(parameters.getExpressions().size() != 2) {
					throw new SQLSyntaxErrorException("TO_DATE with invalid number of parameters");
				}
				return ToDateUtils.resolveToDate((String)evaluateValueExpression(parameters.getExpressions().get(0)), (String)evaluateValueExpression(parameters.getExpressions().get(1)));
			case "NOW":
			case "GETDATE":
			case "CURDATE":
			case "SYSDATE":
				return LocalDateTime.now(ZoneId.systemDefault());
			case "TRUNC":
				if(parameters.getExpressions().get(0) instanceof Column && ((Column)parameters.getExpressions().get(0)).getColumnName().equalsIgnoreCase("SYSDATE")) {
					return LocalDateTime.now(ZoneId.systemDefault());
				} else if(parameters.getExpressions().get(0) instanceof Function) {
					Function nestedFunction = (Function)parameters.getExpressions().get(0);
					if(nestedFunction.getName().equalsIgnoreCase("SYSDATE")  
							|| nestedFunction.getName().equalsIgnoreCase("GETDATE")
							|| nestedFunction.getName().equalsIgnoreCase("CURDATE")
							|| nestedFunction.getName().equalsIgnoreCase("NOW")) {
						return LocalDateTime.now(ZoneId.systemDefault());
					}
				}
				throw new SQLSyntaxErrorException(String.format("'%s' unsupported", function.toString()));
		}
		throw new SQLSyntaxErrorException(String.format("Function '%s' unsupported", function.getName()));
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
