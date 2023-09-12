package org.fpasti.jdbc.esqlj;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import org.fpasti.jdbc.esqlj.testUtils.ElasticLiveEnvironment;
import org.fpasti.jdbc.esqlj.testUtils.ElasticLiveUnit;
import org.fpasti.jdbc.esqlj.testUtils.TestUtils;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

/**
* @author  Fabrizio Pasti - fabrizio.pasti@gmail.com
*/

@ElasticLiveUnit
@ExtendWith(ElasticLiveEnvironment.class)
public class TestLiveUpdate
{
	@Test
	public void update() throws SQLException {
//		Statement stmt = TestUtils.getLiveConnection().createStatement();
//		stmt.execute(TestUtils.resolveTestIndex("UPDATE \"goiot-device\"\r\n"
//				+ "	SET name='西西俱乐部无风2', state='offline'"
//				+ "	WHERE \"_id\"='xixiBikeClub';\r\n"
//				+ ""));
//		stmt.close();
	}
	
	@Test
	public void insert() throws SQLException {
//		Statement stmt = TestUtils.getLiveConnection().createStatement();
//		stmt.execute(TestUtils.resolveTestIndex("insert into \"goiot-device\" (id, name, state) values( "
//				+ "	'test2', '西西俱乐部无风2','offline')"
//				+ ""));
//		stmt.close();
	}
	
	@Test
    public void drop() throws SQLException {
//        Statement stmt = TestUtils.getLiveConnection().createStatement();
//        stmt.execute(TestUtils.resolveTestIndex("DROP TABLE testIndex"));
//        stmt.close();
    }
	
	@Test
    public void delete() throws SQLException {
//        Statement stmt = TestUtils.getLiveConnection().createStatement();
//        stmt.execute(TestUtils.resolveTestIndex("delete from \"goiot-devicelogs-mqttserver-202306\" where _id = '4XiHhYgBhOJQaUsJR8FQ'"));
//        stmt.close();
    }
}
