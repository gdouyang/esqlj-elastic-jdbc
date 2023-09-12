package org.fpasti.jdbc.esqlj.elastic.query.statement;

import java.sql.SQLException;
import java.sql.SQLSyntaxErrorException;
import java.util.ArrayList;
import java.util.List;
import org.fpasti.jdbc.esqlj.elastic.query.impl.search.clause.ClauseWhere;
import org.fpasti.jdbc.esqlj.elastic.query.impl.search.clause.utils.ExpressionResolverValue;
import org.fpasti.jdbc.esqlj.elastic.query.statement.model.ExpressionEnum;
import org.fpasti.jdbc.esqlj.elastic.query.statement.model.Index;
import org.fpasti.jdbc.esqlj.elastic.query.statement.model.QueryColumn;
import net.sf.jsqlparser.expression.Alias;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.operators.relational.EqualsTo;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.delete.Delete;

/**
 * @author Fabrizio Pasti - fabrizio.pasti@gmail.com
 */

public class SqlStatementDelete extends SqlStatement implements IWhereCondition {
  private List<Index> indices;
  private String id;

  private Expression where;
  
  public SqlStatementDelete(Delete statement, List<Object> parameters) throws SQLException {
    super(SqlStatementType.DELETE, parameters);

    Table table = statement.getTable();
    Alias alias = table.getAlias();
    index = new Index(table.getName().replaceAll("\"", ""),
        alias != null ? alias.getName().replaceAll("\"", "") : null);
    indices = new ArrayList<Index>();
    indices.add(index);

    this.where = statement.getWhere();
    ExpressionEnum resolveByInstance = ExpressionEnum.resolveByInstance(where);

    if (resolveByInstance != ExpressionEnum.EQUALS_TO) {
      throw new SQLException("_id must persent");
    }

    EqualsTo equalsTo = (EqualsTo) where;
    String column = ClauseWhere.getColumn(equalsTo.getLeftExpression(), this);
    if (!"_id".equals(column)) {
      throw new SQLSyntaxErrorException(
          String.format("Unsupported WHERE expression: %s", where.toString()));
    }
    this.id = String
        .valueOf(ExpressionResolverValue.evaluateValueExpression(equalsTo.getRightExpression(), this));
  }

  public String getId() {
    return this.id;
  }

  public Expression getWhereCondition() {
    return where;
  }

  public QueryColumn getColumnsByNameOrAlias(String columnName) {
    return new QueryColumn(columnName, columnName, index.getName());
  }
  

}
