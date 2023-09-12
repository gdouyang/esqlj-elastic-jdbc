package org.fpasti.jdbc.esqlj.elastic.query.statement;

import java.util.ArrayList;
import java.util.List;
import org.fpasti.jdbc.esqlj.elastic.query.statement.model.Index;
import net.sf.jsqlparser.expression.Alias;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.drop.Drop;

/**
 * @author Fabrizio Pasti - fabrizio.pasti@gmail.com
 */

public class SqlStatementDrop extends SqlStatement {
  private List<Index> indices;

  public SqlStatementDrop(Drop statement, List<Object> parameters) {
    super(SqlStatementType.DROP, parameters);

    Table table = statement.getName();
    Alias alias = table.getAlias();
    index = new Index(table.getName().replaceAll("\"", ""),
        alias != null ? alias.getName().replaceAll("\"", "") : null);
    indices = new ArrayList<Index>();
    indices.add(index);
  }

}
