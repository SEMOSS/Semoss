package prerna.sablecc2.reactor.export;

import java.util.List;

import prerna.algorithm.api.ITableDataFrame;
import prerna.ds.nativeframe.NativeFrame;
import prerna.query.interpreters.IQueryInterpreter;
import prerna.query.querystruct.AbstractQueryStruct.QUERY_STRUCT_TYPE;
import prerna.query.querystruct.HardSelectQueryStruct;
import prerna.query.querystruct.SelectQueryStruct;
import prerna.query.querystruct.transform.QSAliasToPhysicalConverter;
import prerna.sablecc2.om.GenRowStruct;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.sablecc2.reactor.AbstractReactor;

public class ConvertToQueryReactor extends AbstractReactor {

	@Override
	public NounMetadata execute() {
		String query = null;

		SelectQueryStruct qs = getQs();
		
		// if query defined
		// just grab it
		if(qs instanceof HardSelectQueryStruct) {
			query = ((HardSelectQueryStruct) qs).getQuery();
			return new NounMetadata(query, PixelDataType.CONST_STRING);
		}
		
		// else, we grab the interpreter 
		// from the engine or frame
		// if frame - must convert to physical from alias
		IQueryInterpreter interp = null;
		if(qs.getQsType() == QUERY_STRUCT_TYPE.ENGINE) {
			interp = qs.retrieveQueryStructEngine().getQueryInterpreter();
		} else if(qs.getQsType() == QUERY_STRUCT_TYPE.FRAME) {
			ITableDataFrame frame = qs.getFrame();
			interp = frame.getQueryInterpreter();
			if(frame instanceof NativeFrame) {
				qs = ((NativeFrame) frame).prepQsForExecution(qs);
			} else {
				qs = QSAliasToPhysicalConverter.getPhysicalQs(qs, frame.getMetaData());
			}
		} else {
			throw new IllegalArgumentException("Cannot generate a query for this source");
		}
		
		interp.setQueryStruct(qs);
		// grab the query
		query = interp.composeQuery();
		return new NounMetadata(query, PixelDataType.CONST_STRING);
	}

	private SelectQueryStruct getQs() {
		GenRowStruct grsQs = this.store.getNoun(PixelDataType.QUERY_STRUCT.getKey());
		NounMetadata noun;
		//if we don't have tasks in the curRow, check if it exists in genrow under the qs key
		if(grsQs != null && !grsQs.isEmpty()) {
			noun = grsQs.getNoun(0);
			return (SelectQueryStruct) noun.getValue();
		} else {
			List<NounMetadata> qsList = this.curRow.getNounsOfType(PixelDataType.QUERY_STRUCT);
			if(qsList != null && !qsList.isEmpty()) {
				noun = qsList.get(0);
				return (SelectQueryStruct) noun.getValue();
			}
		}
		
		return null;
	}

}
