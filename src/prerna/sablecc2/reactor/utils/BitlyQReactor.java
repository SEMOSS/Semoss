package prerna.sablecc2.reactor.utils;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import prerna.engine.impl.rdbms.RDBMSNativeEngine;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.sablecc2.reactor.AbstractReactor;
import prerna.util.Constants;
import prerna.util.Utility;

public class BitlyQReactor extends AbstractReactor {
	
	public BitlyQReactor() {
		this.keysToGet = new String[]{"fancy"};
	}

	@Override
	public NounMetadata execute() {

		organizeKeys();
		RDBMSNativeEngine engine = (RDBMSNativeEngine) Utility.getEngine(Constants.LOCAL_MASTER_DB_NAME);
		Connection conn = engine.makeConnection();
		
		String embed = "No URL Found for " + this.keyValue.get("fancy");
		
		try {
			// check to see if such a fancy name exists
			Statement stmt = conn.createStatement();
			String query = "SELECT embed from bitly where fancy='" + this.keyValue.get("fancy") + "'";
			ResultSet rs = stmt.executeQuery(query);
			// if there is a has next not sure what
			
			if(rs.next())
			{
				embed = this.keyValue.get("fancy") + " <>" + rs.getString(1);
			}
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return new NounMetadata(embed, PixelDataType.CONST_STRING);
	}
}
