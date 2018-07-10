package prerna.sablecc2.reactor.qs;

import prerna.engine.api.IEngine;
import prerna.engine.impl.rdbms.RDBMSNativeEngine;
import prerna.query.querystruct.SelectQueryStruct;
import prerna.query.querystruct.delete.DeleteSqlInterpreter;
import prerna.query.querystruct.update.UpdateQueryStruct;
import prerna.query.querystruct.update.UpdateSqlInterpreter;
import prerna.sablecc2.om.GenRowStruct;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.sablecc2.reactor.AbstractReactor;

public class ExecQueryReactor extends AbstractReactor {

	private NounMetadata qStruct = null;
	
	public ExecQueryReactor() {
		this.keysToGet = new String[]{ReactorKeysEnum.QUERY_STRUCT.getKey()};
	}
	
	@Override
	public NounMetadata execute() {
		if(qStruct == null) {
			qStruct = getQueryStruct();
		}
		
		IEngine engine = null;
		SelectQueryStruct sQS = null;
		UpdateQueryStruct uQS = null;
		boolean success = false;

		if(qStruct.getValue() instanceof SelectQueryStruct) {
			sQS = ((SelectQueryStruct) qStruct.getValue());
			engine = sQS.retrieveQueryStructEngine();
		} else {
			uQS = ((UpdateQueryStruct) qStruct.getValue());
			engine = uQS.retrieveQueryStructEngine();
		}

		if(!(engine instanceof RDBMSNativeEngine)) {
			throw new IllegalArgumentException("Insert query only works for rdbms databases");
		}

		if(sQS != null) {
			DeleteSqlInterpreter interp = new DeleteSqlInterpreter(sQS);
			String sqlQuery = interp.composeQuery();
			System.out.println("SQL QUERY...." + sqlQuery);
			engine.insertData(sqlQuery);
			success = true;
		} else {
			UpdateSqlInterpreter interp = new UpdateSqlInterpreter(uQS);
			String sqlQuery = interp.composeQuery();
			System.out.println("SQL QUERY...." + sqlQuery);
			engine.insertData(sqlQuery);
			success = true;
		}
		
		return new NounMetadata(success, PixelDataType.BOOLEAN);
	}
	
	private NounMetadata getQueryStruct() {
		
		NounMetadata object = new NounMetadata(null, PixelDataType.QUERY_STRUCT);
		GenRowStruct allNouns = getNounStore().getNoun(PixelDataType.QUERY_STRUCT.toString());
		NounMetadata f = new NounMetadata(false, PixelDataType.BOOLEAN);
		if(allNouns != null) {
			object = (NounMetadata)allNouns.getNoun(0);
			return object;
		}
		return f;
	}
	
	public void setQueryStruct(NounMetadata qs) {
		this.qStruct = qs;
	}
}
