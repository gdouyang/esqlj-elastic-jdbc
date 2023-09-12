package org.fpasti.jdbc.esqlj;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.sql.SQLWarning;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

import org.fpasti.jdbc.esqlj.elastic.query.Executor;

/**
* @author  Fabrizio Pasti - fabrizio.pasti@gmail.com
*/

public class EsStatement implements Statement {

	protected EsConnection connection;
	protected EsResultSet resultSet;
	
	protected List<Object>parameters;

	public EsStatement(EsConnection connection) {
		this.connection = connection;
		parameters = new ArrayList<>();
	}
 
	@Override
	public <T> T unwrap(Class<T> iface) throws SQLException {
		return iface.cast(this);
	}

	@Override
	public boolean isWrapperFor(Class<?> iface) throws SQLException {
		return iface.isInstance(this);
	}

	@Override
	public void close() throws SQLException {
		if(resultSet != null) {
			resultSet.close();
		}
	}

	@Override
	public int getMaxFieldSize() throws SQLException {
		return 0;
	}

	@Override
	public void setMaxFieldSize(int max) throws SQLException {
		// not implemented
	}

	@Override
	public int getMaxRows() throws SQLException {
		return 0;
	}

	@Override
	public void setMaxRows(int max) throws SQLException {
	}

	@Override
	public void setEscapeProcessing(boolean enable) throws SQLException {
	}

	@Override
	public int getQueryTimeout() throws SQLException {
		// not supported
		return 0;
	}

	@Override
	public void setQueryTimeout(int seconds) throws SQLException {
		// not supported
	}

	@Override
	public void cancel() throws SQLException {
		// not supported
	}

	@Override
	public SQLWarning getWarnings() throws SQLException {
		return null;
	}

	@Override
	public void clearWarnings() throws SQLException {
	}

	@Override
	public void setCursorName(String name) throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public boolean execute(String sql) throws SQLException {
		resultSet = new EsResultSet(Executor.execSql(connection, sql, parameters));
		return !resultSet.getInternalQuery().isEmpty(); // todo: add also if it is an update query
	}

	@Override
	public ResultSet getResultSet() throws SQLException {
		return resultSet;
	}

	@Override
	public int getUpdateCount() throws SQLException {
		return -1;
	}

	@Override
	public boolean getMoreResults() throws SQLException {
		return resultSet.next();
	}

	@Override
	public void setFetchDirection(int direction) throws SQLException {
		resultSet.setFetchDirection(direction);
	}

	@Override
	public int getFetchDirection() throws SQLException {
		return resultSet.getFetchDirection();
	}

	@Override
	public void setFetchSize(int rows) throws SQLException {
		resultSet.setFetchSize(rows);
	}

	@Override
	public int getFetchSize() throws SQLException {
		return resultSet.getFetchSize();
	}

	@Override
	public int getResultSetConcurrency() throws SQLException {
		return ResultSet.CONCUR_READ_ONLY;
	}

	@Override
	public int getResultSetType() throws SQLException {
		return ResultSet.TYPE_FORWARD_ONLY;
	}

	@Override
	public void addBatch(String sql) throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public void clearBatch() throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public int[] executeBatch() throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public Connection getConnection() throws SQLException {
		return connection;
	}

	@Override
	public boolean getMoreResults(int current) throws SQLException {
		return getMoreResults();
	}

	@Override
	public ResultSet getGeneratedKeys() throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}
	
	@Override
	public ResultSet executeQuery(String sql) throws SQLException {
		resultSet = new EsResultSet(Executor.execSql(connection, sql, parameters));
		return resultSet;
	}

	@Override
	public int executeUpdate(String sql) throws SQLException {
		resultSet = new EsResultSet(Executor.execSql(connection, sql, parameters));
		return 1;
	}

	@Override
	public int executeUpdate(String sql, int autoGeneratedKeys) throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public int executeUpdate(String sql, int[] columnIndexes) throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public int executeUpdate(String sql, String[] columnNames) throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public boolean execute(String sql, int autoGeneratedKeys) throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public boolean execute(String sql, int[] columnIndexes) throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public boolean execute(String sql, String[] columnNames) throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public int getResultSetHoldability() throws SQLException {
		return 0;
	}

	@Override
	public boolean isClosed() throws SQLException {
		return resultSet.isClosed();
	}

	@Override
	public void setPoolable(boolean poolable) throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public boolean isPoolable() throws SQLException {
		return false;
	}

	@Override
	public void closeOnCompletion() throws SQLException {
		resultSet.setCloseable(true);
	}

	@Override
	public boolean isCloseOnCompletion() throws SQLException {
		return resultSet.isCloseable();
	}

}
