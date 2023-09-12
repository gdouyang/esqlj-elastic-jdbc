package org.fpasti.jdbc.esqlj.elastic.query.impl;

import java.io.IOException;
import java.sql.ResultSetMetaData;
import java.sql.RowId;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.sql.SQLSyntaxErrorException;
import java.util.logging.Level;

import org.fpasti.jdbc.esqlj.EsConnection;
import org.fpasti.jdbc.esqlj.elastic.query.AbstractQuery;
import org.fpasti.jdbc.esqlj.elastic.query.statement.SqlStatementDrop;
import org.fpasti.jdbc.esqlj.support.EsRuntimeException;
import co.elastic.clients.elasticsearch.indices.DeleteIndexRequest;
import co.elastic.clients.elasticsearch.indices.DeleteIndexResponse;

/**
* @author  Fabrizio Pasti - fabrizio.pasti@gmail.com
*/

public class ElasticDrop extends AbstractQuery {

	public ElasticDrop(EsConnection connection, SqlStatementDrop drop) throws SQLException {
		super(connection, drop.getIndex().getName());
		initialFetch(drop);
	}
	
	private void initialFetch(SqlStatementDrop select) throws SQLException {
		try {
		    DeleteIndexRequest request = new DeleteIndexRequest.Builder().index(select.getIndex().getName()).build();
		    if (logger.isLoggable(Level.INFO)) {
		    	logger.info("request data= " + request.toString());
		    }
			DeleteIndexResponse resp = getConnection().getElasticClient().indices().delete(request);
			if (logger.isLoggable(Level.INFO)) {
		      logger.info("resp data= " + resp);
		    }
		} catch(EsRuntimeException ere) {
			throw new SQLSyntaxErrorException(ere.getMessage());
		} catch(IOException e) {
			throw new SQLException(e.getMessage());
		}
	}


	@Override
	public boolean next() throws SQLException {
	  return true;
	}


	@Override
	public boolean isBeforeFirst() {
		return true;
	}

	@Override
	public boolean isFirst() {
	  return true;
	}
	
	@Override
	public boolean isLast() {
	  return true;
	}

	@Override
	public void reset() throws SQLException {
	}

	@Override
	public void finish() throws SQLException {
	}

	@Override
	public boolean moveToFirst() throws SQLException {
		return true;
	}

	@Override
	public boolean moveToLast() throws SQLException {
		return true;
	}

	@Override
	public int getCurrentRowIndex() throws SQLException {
		return 1;
	}
	
	@Override
	public boolean moveToRow(int rowIndex) throws SQLException {
		return true;
	}

	@Override
	public boolean isProvidingData() {
	  return true;
	}

	@Override
	public boolean moveByDelta(int rows) throws SQLException {
		return true;
	}

	@Override
	public void setIterationStep(int iterationStep) {
	}

	@Override
	public void setFetchSize(int size) {
	}

	@Override
	public int getFetchSize() {
		return 1; 
	}

	@Override
	public boolean isForwardOnly() {
		return false;
	}

	@Override
	public void close() throws SQLException {
	}

	@Override
	public <T> T getColumnValue(int columnIndex, Class<T> type) throws SQLException {
		return null;
	}

	@Override
	public <T> T getColumnValue(String columnName, Class<T> type) throws SQLException {
		return null;
	}

	@Override
	public ResultSetMetaData getResultSetMetaData() {
		return null;
	}

	@Override
	public Object getColumnValue(int columnIndex) throws SQLException {
		return null;
	}

	@Override
	public Object getColumnValue(String columnName) throws SQLException {
		return null;
	}

	@Override
	public int findColumnIndex(String columnLabel) {
		return 0;
	}

	@Override
	public RowId getRowId() throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public boolean isEmpty() {
		return true;
	}

}
