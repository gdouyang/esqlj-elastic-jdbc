package org.fpasti.jdbc.esqlj.elastic.query.statement;

import java.util.List;

import org.fpasti.jdbc.esqlj.elastic.query.statement.model.Index;

/**
* @author  Fabrizio Pasti - fabrizio.pasti@gmail.com
*/

public class SqlStatement {
	private SqlStatementType type;
	protected Index index;
	protected List<Object> parameters;
	public SqlStatement(SqlStatementType type, List<Object> parameters) {
		super();
		this.type = type;
		this.parameters = parameters;
	}

	public SqlStatementType getType() {
		return type;
	}

	public Index getIndex() {
		return index;
	}

	public List<Object> getParameters() {
		return this.parameters;
	}
}
