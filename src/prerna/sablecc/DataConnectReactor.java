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

public class DataConnectReactor extends AbstractReactor {

	public DataConnectReactor() {
		String[] thisReacts = { PKQLEnum.WORD_OR_NUM};
		super.whatIReactTo = thisReacts;
		super.whoAmI = PKQLEnum.DATA_CONNECT;
	}

	@Override
	public Iterator process() {
		Object value = myStore.get(PKQLEnum.WORD_OR_NUM);
		System.out.println(value);//Testing the value passed in pkql data.connect(value)

		AbstractTableDataFrame frame = (AbstractTableDataFrame) myStore.get("G");
		if (frame instanceof H2Frame) {
			Connection currConn = ((H2Frame) frame).getConn();
			try {
				DatabaseMetaData dmd = currConn.getMetaData();
				myStore.put("data.connect", dmd.getURL());
				myStore.put("STATUS", PKQLRunner.STATUS.SUCCESS);
			} catch (SQLException e) {
				e.printStackTrace();
				myStore.put("data.connect", currConn.toString());
				myStore.put("STATUS", PKQLRunner.STATUS.ERROR);
			}

		} else if (frame instanceof TinkerFrame) {
			myStore.put("data.connect", "JDBC URL not available for TinkerFrame");
			myStore.put("STATUS", PKQLRunner.STATUS.ERROR);
		} 
//		else if (frame instanceof SparkDataFrame) {
//			myStore.put("data.connect", "JDBC URL not available for SparkDataFrame");
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
