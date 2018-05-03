package prerna.query.querystruct.update.reactors;

import prerna.engine.api.IEngine;
import prerna.engine.impl.rdbms.RDBMSNativeEngine;
import prerna.query.querystruct.update.UpdateQueryStruct;
import prerna.query.querystruct.update.UpdateSqlInterpreter;
import prerna.sablecc2.om.GenRowStruct;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.sablecc2.reactor.AbstractReactor;

public class ExecUpdateReactor extends AbstractReactor {

	public ExecUpdateReactor() {
		this.keysToGet = new String[]{ReactorKeysEnum.QUERY_STRUCT.getKey()};
	}
	
	@Override
	public NounMetadata execute() {
		UpdateQueryStruct qs = getQueryStruct();
		IEngine engine = qs.retrieveQueryStructEngine();
		boolean success = false;
		if(engine instanceof RDBMSNativeEngine) {
			UpdateSqlInterpreter interp = new UpdateSqlInterpreter(qs);
			String sqlQuery = interp.composeQuery();
			engine.insertData(sqlQuery);
			success = true;
		}
		
		return new NounMetadata(success, PixelDataType.BOOLEAN);
	}
	
	private UpdateQueryStruct getQueryStruct() {
		GenRowStruct allNouns = getNounStore().getNoun(PixelDataType.QUERY_STRUCT.toString());
		UpdateQueryStruct queryStruct = null;
		if(allNouns != null) {
			NounMetadata object = (NounMetadata)allNouns.getNoun(0);
			return (UpdateQueryStruct)object.getValue();
		} 
		return queryStruct;
	}
}
