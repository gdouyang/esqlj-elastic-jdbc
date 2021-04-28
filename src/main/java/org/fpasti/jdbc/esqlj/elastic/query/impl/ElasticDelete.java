package org.fpasti.jdbc.esqlj.elastic.query.impl;

import java.io.IOException;
import java.sql.ResultSetMetaData;
import java.sql.RowId;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.sql.SQLSyntaxErrorException;
import org.fpasti.jdbc.esqlj.EsConnection;
import org.fpasti.jdbc.esqlj.elastic.query.AbstractQuery;
import org.fpasti.jdbc.esqlj.elastic.query.statement.SqlStatementDelete;
import org.fpasti.jdbc.esqlj.support.EsRuntimeException;
import co.elastic.clients.elasticsearch.core.DeleteRequest;
import co.elastic.clients.elasticsearch.core.DeleteResponse;

/**
 * @author Fabrizio Pasti - fabrizio.pasti@gmail.com
 */

public class ElasticDelete extends AbstractQuery {

  public ElasticDelete(EsConnection connection, SqlStatementDelete delete) throws SQLException {
    super(connection, delete.getIndex().getName());
    initialFetch(delete);
  }

  private void initialFetch(SqlStatementDelete delete) throws SQLException {
    try {
      DeleteRequest request = new DeleteRequest.Builder()//
          .index(delete.getIndex().getName())//
          .id(delete.getId()).build();
      System.out.println("request data= " + request.toString());
      DeleteResponse resp = getConnection().getElasticClient().delete(request);
      System.out.println(resp);
    } catch (EsRuntimeException ere) {
      throw new SQLSyntaxErrorException(ere.getMessage());
    } catch (IOException e) {
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
  public void reset() throws SQLException {}

  @Override
  public void finish() throws SQLException {}

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
  public void setIterationStep(int iterationStep) {}

  @Override
  public void setFetchSize(int size) {}

  @Override
  public int getFetchSize() {
    return 1;
  }

  @Override
  public boolean isForwardOnly() {
    return false;
  }

  @Override
  public void close() throws SQLException {}

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
