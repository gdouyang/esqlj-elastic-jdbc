package org.fpasti.jdbc.esqlj.elastic.query.statement;

import java.util.List;

import org.fpasti.jdbc.esqlj.elastic.query.statement.model.QueryColumn;
import net.sf.jsqlparser.expression.Expression;

public interface IWhereCondition {
  public Expression getWhereCondition();
  
  public QueryColumn getColumnsByNameOrAlias(String columnName);
  
  public List<Object> getParameters();
}
