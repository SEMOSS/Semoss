package prerna.sablecc;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.SQLException;
import java.util.Iterator;

import prerna.ds.TinkerFrame;
import prerna.ds.rdbms.h2.H2Frame;
import prerna.ds.shared.AbstractTableDataFrame;
//import prerna.ds.spark.SparkDataFrame;
import prerna.sablecc.meta.IPkqlMetadata;

public class DataDisconnectReactor  extends AbstractReactor{

	@Override
	public Iterator process() {
		System.out.println("Inside DataDisconnectReactor.process()");
		AbstractTableDataFrame frame = (AbstractTableDataFrame) myStore.get("G");
		if(frame instanceof H2Frame){
			Connection currConn = ((H2Frame) frame).getConn();
			if(currConn != null){
				try {
					if(!currConn.isClosed()){
						DatabaseMetaData dmd = currConn.getMetaData();
						String connUrl = dmd.getURL();
						currConn.close();
						myStore.put("data.disconnect", "Connection closed for " + connUrl);
						myStore.put("STATUS", PKQLRunner.STATUS.SUCCESS);
					}else{
						myStore.put("data.disconnect", "Connection closed for " + currConn.toString());
						myStore.put("STATUS", PKQLRunner.STATUS.SUCCESS);
					}
				} catch (SQLException e) {
					e.printStackTrace();
					myStore.put("data.disconnect", "Error in closing connection " + currConn.toString() + " due to " + e.getMessage());
					myStore.put("STATUS", PKQLRunner.STATUS.ERROR);
				}
			}

		}else if(frame instanceof TinkerFrame){
			myStore.put("data.disconnect", "JDBC URL not available for TinkerFrame");
			myStore.put("STATUS", PKQLRunner.STATUS.ERROR);
		}
//		else if(frame instanceof SparkDataFrame){
//			myStore.put("data.disconnect", "JDBC URL not available for SparkDataFrame");
//			myStore.put("STATUS", PKQLRunner.STATUS.ERROR);
//		}
		return null;
	}

	@Override
	public IPkqlMetadata getPkqlMetadata() {
		// TODO Auto-generated method stub
		return null;
	}




}
