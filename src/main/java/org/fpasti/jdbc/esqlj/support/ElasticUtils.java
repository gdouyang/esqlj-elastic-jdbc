package org.fpasti.jdbc.esqlj.support;

import java.io.IOException;
import java.sql.SQLNonTransientConnectionException;
import org.apache.http.ParseException;
import org.fpasti.jdbc.esqlj.Configuration;
import org.fpasti.jdbc.esqlj.ConfigurationPropertyEnum;
import org.fpasti.jdbc.esqlj.EsConnection;
import org.fpasti.jdbc.esqlj.elastic.query.impl.search.RequestInstance;
import co.elastic.clients.elasticsearch._types.Time;
import co.elastic.clients.elasticsearch.core.ClosePointInTimeRequest;
import co.elastic.clients.elasticsearch.core.OpenPointInTimeRequest;

/**
* @author  Fabrizio Pasti - fabrizio.pasti@gmail.com
*/

public class ElasticUtils {

	public static String getPointInTime(EsConnection connection, RequestInstance req) throws SQLNonTransientConnectionException  {
		try {
			Long configuration = Configuration.getConfiguration(ConfigurationPropertyEnum.CFG_QUERY_SCROLL_TIMEOUT_MINUTES, Long.class);
			
			Time time = new Time.Builder().time(String.format("%dm", configuration)).build();
			OpenPointInTimeRequest request = new OpenPointInTimeRequest.Builder().index(req.getIndexMetaData().getIndex()).keepAlive(time).build();
			String id = connection.getElasticClient().openPointInTime(request).id();
		    return id;
		} catch(ParseException | IOException e) {
			throw new SQLNonTransientConnectionException(e);
		}
	}
	
	public static void deletePointInTime(EsConnection connection, String pit) {
		try {
		    ClosePointInTimeRequest request = new ClosePointInTimeRequest.Builder().id(pit).build();
		    connection.getElasticClient().closePointInTime(request).succeeded();			
		} catch(ParseException | IOException e) {
			// nop
		}		
	}

	
}
