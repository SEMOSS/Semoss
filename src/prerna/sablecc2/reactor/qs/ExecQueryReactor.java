package prerna.sablecc2.reactor.qs;

import prerna.algorithm.api.ITableDataFrame;
import prerna.ds.h2.H2Frame;
import prerna.engine.api.IEngine;
import prerna.engine.impl.rdbms.RDBMSNativeEngine;
import prerna.query.querystruct.AbstractQueryStruct;
import prerna.query.querystruct.AbstractQueryStruct.QUERY_STRUCT_TYPE;
import prerna.query.querystruct.SelectQueryStruct;
import prerna.query.querystruct.delete.DeleteSqlInterpreter;
import prerna.query.querystruct.transform.QSAliasToPhysicalConverter;
import prerna.query.querystruct.update.UpdateQueryStruct;
import prerna.query.querystruct.update.UpdateSqlInterpreter;
import prerna.sablecc2.om.GenRowStruct;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
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
		ITableDataFrame frame = null;
		AbstractQueryStruct qs = null;
		boolean success = false;

		if(qStruct.getValue() instanceof AbstractQueryStruct) {
			qs = ((AbstractQueryStruct) qStruct.getValue());
			if(qs.getQsType() == QUERY_STRUCT_TYPE.ENGINE) {
				engine = qs.retrieveQueryStructEngine();
				if(!(engine instanceof RDBMSNativeEngine)) {
					throw new IllegalArgumentException("Query update/deletes only works for rdbms databases");
				}
			} else if(qs.getQsType() == QUERY_STRUCT_TYPE.FRAME) {
				frame = qs.getFrame();
				if(!(frame instanceof H2Frame)) {
					throw new IllegalArgumentException("Query update/deletes only works for sql frames");
				}
				// convert any aliases the FE is using from the frame
				qs = QSAliasToPhysicalConverter.getPhysicalQs(qs, frame.getMetaData());
			}
		} else {
			throw new IllegalArgumentException("Input to exec query requires a query struct");
		}

		String query = null;
		if(qs instanceof UpdateQueryStruct) {
			UpdateSqlInterpreter interp = new UpdateSqlInterpreter((UpdateQueryStruct) qs);
			query = interp.composeQuery();
		} else if(qs instanceof SelectQueryStruct) {
			DeleteSqlInterpreter interp = new DeleteSqlInterpreter((SelectQueryStruct) qs);
			query = interp.composeQuery();
		}
		
		System.out.println("SQL QUERY...." + query);
		if(qs.getQsType() == QUERY_STRUCT_TYPE.ENGINE) {
			engine.insertData(query);
			success = true;
		} else {
			try {
				((H2Frame) frame).getBuilder().runQuery(query);
				success = true;
			} catch (Exception e) {
				success = false;
				e.printStackTrace();
			}
		}
		
		return new NounMetadata(success, PixelDataType.BOOLEAN, PixelOperationType.ALTER_DATABASE);
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
