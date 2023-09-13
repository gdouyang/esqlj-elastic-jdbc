package org.fpasti.jdbc.esqlj.elastic.query.statement;

import java.sql.SQLException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.fpasti.jdbc.esqlj.elastic.query.impl.search.clause.utils.ExpressionResolverValue;
import org.fpasti.jdbc.esqlj.elastic.query.statement.model.Index;
import org.fpasti.jdbc.esqlj.elastic.query.statement.model.QueryColumn;

import co.elastic.clients.elasticsearch._types.InlineScript;
import co.elastic.clients.elasticsearch._types.Script;
import co.elastic.clients.json.JsonData;
import net.sf.jsqlparser.expression.Alias;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.update.Update;

/**
* @author  Fabrizio Pasti - fabrizio.pasti@gmail.com
*/

public class SqlStatementUpdate extends SqlStatement implements IWhereCondition {

	private Expression where;
	
	private Script script;
	
	public SqlStatementUpdate(Update statement, List<Object> parameters) throws SQLException {
		super(SqlStatementType.SELECT, parameters);
		
	    Table table = statement.getTable();
	    Alias alias = table.getAlias();
	    index = new Index(table.getName().replaceAll("\"", ""),
	        alias != null ? alias.getName().replaceAll("\"", "") : null);
	    
		this.where = statement.getWhere();
		
		List<Column> columns = statement.getColumns();
		List<Expression> expressions = statement.getExpressions();
		Map<String, JsonData> p = new HashMap<>();
		StringBuilder sb = new StringBuilder();
		for (int i = 0; i < columns.size(); i++) {
			Column column = columns.get(i);
			Expression expression = expressions.get(i);
			
			String columnName = this.getColumnsByNameOrAlias(column.getColumnName()).getName();
			JsonData value = JsonData.of(ExpressionResolverValue.evaluateValueExpression(expression, this));
			p.put(columnName, value);
			sb.append(String.format("ctx._source.%s = params.%s;", columnName, columnName));
		}
		script = new Script.Builder().inline(new InlineScript.Builder()
				.params(p)
				.source(sb.toString())
				.build())
				.build();
	}

	@Override
	public Expression getWhereCondition() {
		return where;
	}

	@Override
	public QueryColumn getColumnsByNameOrAlias(String columnName) {
		return new QueryColumn(columnName, columnName, index.getName());
	}
	
	public Script getScript() {
		return script;
	}
	
}
