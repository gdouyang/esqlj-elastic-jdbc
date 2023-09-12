package org.fpasti.jdbc.esqlj.elastic.query.statement;

import java.sql.SQLException;
import java.sql.SQLSyntaxErrorException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.fpasti.jdbc.esqlj.elastic.query.impl.search.clause.utils.ExpressionResolverValue;
import org.fpasti.jdbc.esqlj.elastic.query.statement.model.Index;

import net.sf.jsqlparser.expression.Alias;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.operators.relational.ExpressionList;
import net.sf.jsqlparser.expression.operators.relational.ItemsList;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.insert.Insert;

/**
* @author  Fabrizio Pasti - fabrizio.pasti@gmail.com
*/

public class SqlStatementInsert extends SqlStatement {

	private Map<String, Object> doc;
	public SqlStatementInsert(Insert statement) throws SQLException {
		super(SqlStatementType.INSERT);
		
		Table table = statement.getTable();
	    Alias alias = table.getAlias();
	    index = new Index(table.getName().replaceAll("\"", ""),
	        alias != null ? alias.getName().replaceAll("\"", "") : null);
	    
		doc = new HashMap<>();
		
		List<Column> columns = statement.getColumns();
		ItemsList itemsList = statement.getItemsList();
		if (itemsList instanceof ExpressionList) {
			ExpressionList expList = (ExpressionList)itemsList;
			List<Expression> expressions = expList.getExpressions();
			for (int i = 0; i < columns.size(); i++) {
				Column column = columns.get(i);
				String columnName = column.getColumnName();
				
				Expression expression = expressions.get(i);
				doc.put(columnName, ExpressionResolverValue.evaluateValueExpression(expression));
			}
		} else {
			throw new SQLSyntaxErrorException("Unsupport ItemsList " + itemsList.getClass().getName());
		}
	}
	public Map<String, Object> getDoc() {
		return doc;
	}
	
}
