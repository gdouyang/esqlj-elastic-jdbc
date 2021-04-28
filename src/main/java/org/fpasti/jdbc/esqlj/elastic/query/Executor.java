package org.fpasti.jdbc.esqlj.elastic.query;

import java.sql.SQLException;
import java.sql.SQLSyntaxErrorException;

import org.fpasti.jdbc.esqlj.EsConnection;
import org.fpasti.jdbc.esqlj.elastic.query.impl.ElasticDelete;
import org.fpasti.jdbc.esqlj.elastic.query.impl.ElasticDrop;
import org.fpasti.jdbc.esqlj.elastic.query.impl.ElasticQuery;
import org.fpasti.jdbc.esqlj.elastic.query.statement.SqlStatement;
import org.fpasti.jdbc.esqlj.elastic.query.statement.SqlStatementDelete;
import org.fpasti.jdbc.esqlj.elastic.query.statement.SqlStatementDrop;
import org.fpasti.jdbc.esqlj.elastic.query.statement.SqlStatementInsert;
import org.fpasti.jdbc.esqlj.elastic.query.statement.SqlStatementSelect;
import org.fpasti.jdbc.esqlj.elastic.query.statement.SqlStatementUpdate;

import net.sf.jsqlparser.parser.CCJSqlParserUtil;
import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.delete.Delete;
import net.sf.jsqlparser.statement.drop.Drop;
import net.sf.jsqlparser.statement.select.Select;

/**
* @author  Fabrizio Pasti - fabrizio.pasti@gmail.com
*/

public class Executor {
	
	public static AbstractQuery execSql(EsConnection connection, String sql) throws SQLException {
		try {
			Statement statement =  CCJSqlParserUtil.parse(sql);
			switch(statement.getClass().getSimpleName()) {
			case "Select":
				return new ElasticQuery(connection, new SqlStatementSelect((Select)statement));
//			case "Update":
//				return new ElasticUpdate(connection, new SqlStatementUpdate(statement));
//			case "Insert":
//				return new SqlStatementInsert(statement);
			case "Delete":
				return new ElasticDelete(connection, new SqlStatementDelete((Delete)statement));
			case "Drop":
				return new ElasticDrop(connection, new SqlStatementDrop((Drop)statement));
			default:
				throw new SQLSyntaxErrorException("Unrecognized statement [" + statement.getClass().getSimpleName() + "]");
			}
		} catch(SQLException se) {
			throw se;
		} catch(Exception e) {
			throw new SQLSyntaxErrorException(e.getCause() != null ? e.getCause().getMessage() : e.getMessage(), e);
		}
	}
	
}
