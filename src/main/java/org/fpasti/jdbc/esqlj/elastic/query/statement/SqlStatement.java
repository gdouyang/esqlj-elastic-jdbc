package org.fpasti.jdbc.esqlj.elastic.query.statement;

import java.util.List;
import java.util.logging.Logger;

import org.fpasti.jdbc.esqlj.elastic.query.statement.model.Index;

/**
* @author  Fabrizio Pasti - fabrizio.pasti@gmail.com
*/

public class SqlStatement {
	public static Logger logger = Logger.getLogger(Logger.GLOBAL_LOGGER_NAME);
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
