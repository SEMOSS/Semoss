package prerna.sablecc2.reactor.imports;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import prerna.algorithm.api.IMetaData;
import prerna.algorithm.api.ITableDataFrame;
import prerna.engine.api.IHeadersDataRow;
import prerna.query.querystruct.CsvQueryStruct;
import prerna.query.querystruct.ExcelQueryStruct;
import prerna.query.querystruct.QueryStruct2;
import prerna.query.querystruct.selectors.QueryColumnSelector;
import prerna.sablecc2.om.GenRowStruct;
import prerna.sablecc2.om.Join;
import prerna.sablecc2.om.NounMetadata;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.QueryFilter;
import prerna.sablecc2.om.task.ITask;
import prerna.sablecc2.reactor.AbstractReactor;

public class MergeDataReactor extends AbstractReactor {

	@Override
	public NounMetadata execute()  {
		ITableDataFrame frame = (ITableDataFrame) this.insight.getDataMaker();
		// this is greedy execution
		// will not return anything
		// but will update the frame in the pixel planner
		QueryStruct2 qs = getQueryStruct();
		List<Join> joins = this.curRow.getAllJoins();
		// if we have an inner join, add the current values as a filter on the query
		// important for performance on large dbs when the user has already 
		// filtered to small subset
		for(Join j : joins) {
			String q = j.getQualifier();
			String s = j.getSelector();
			String type = j.getJoinType();
			if(type.equals("inner.join") || type.equals("left.outer.join")) {
				// we will add a filter frame existing values in frame
				// but wait... need to make sure an existing filter isn't there
				if(qs.hasFiltered(q)) {
					continue;
				}
				QueryStruct2 filterQs = new QueryStruct2();
				QueryColumnSelector column = new QueryColumnSelector(s);
				filterQs.addSelector(column);
				Iterator<IHeadersDataRow> it = frame.query(filterQs);
				List<Object> values = new ArrayList<Object>();
				while(it.hasNext()) {
					values.add(it.next().getValues()[0]);
				}
				NounMetadata lNoun = new NounMetadata(q, PixelDataType.COLUMN);
				NounMetadata rNoun = null;
				if(frame.getMetaData().getHeaderTypeAsEnum(s) == IMetaData.DATA_TYPES.NUMBER) {
					rNoun = new NounMetadata(values, PixelDataType.CONST_DECIMAL);
				} else {
					rNoun = new NounMetadata(values, PixelDataType.CONST_STRING);
				}
				QueryFilter filter = new QueryFilter(lNoun, "==", rNoun);
				qs.addFilter(filter);
			}
		}
		
		IImporter importer = ImportFactory.getImporter(frame, qs);
		// we reassign the frame because it might have changed
		// this only happens for native frame
		frame = importer.mergeData(joins);
		this.insight.setDataMaker(frame);
		// need to clear the unique col count used by FE for determining the need for math
		frame.clearCachedInfo();
		if(qs.getQsType() == QueryStruct2.QUERY_STRUCT_TYPE.CSV_FILE) {
			storeCsvFileMeta((CsvQueryStruct) qs, this.curRow.getAllJoins());
		} else if(qs.getQsType() == QueryStruct2.QUERY_STRUCT_TYPE.EXCEL_FILE) {
			storeExcelFileMeta((ExcelQueryStruct) qs, this.curRow.getAllJoins());
		}
		
		return new NounMetadata(frame, PixelDataType.FRAME, PixelOperationType.FRAME_DATA_CHANGE, PixelOperationType.FRAME_HEADERS_CHANGE);
	}

	private QueryStruct2 getQueryStruct() {
		GenRowStruct allNouns = getNounStore().getNoun("QUERYSTRUCT");
		QueryStruct2 queryStruct = null;
		if(allNouns != null) {
			NounMetadata object = (NounMetadata)allNouns.getNoun(0);
			return (QueryStruct2)object.getValue();
		} 

		return queryStruct;
	}
	
	private void storeCsvFileMeta(CsvQueryStruct qs, List<Join> joins) {
		FileMeta fileMeta = new FileMeta();
		fileMeta.setFileLoc(qs.getCsvFilePath());
		fileMeta.setDataMap(qs.getColumnTypes());
		fileMeta.setNewHeaders(qs.getNewHeaderNames());
		fileMeta.setPixelString(this.originalSignature);
		fileMeta.setSelectors(qs.getSelectors());
		fileMeta.setType(FileMeta.FILE_TYPE.CSV);
		this.insight.addFileUsedInInsight(fileMeta);
	}
	
	private void storeExcelFileMeta(ExcelQueryStruct qs, List<Join> joins) {
		FileMeta fileMeta = new FileMeta();
		fileMeta.setFileLoc(qs.getExcelFilePath());
		fileMeta.setDataMap(qs.getColumnTypes());
		fileMeta.setSheetName(qs.getSheetName());
		fileMeta.setNewHeaders(qs.getNewHeaderNames());
		fileMeta.setSelectors(qs.getSelectors());
		fileMeta.setTableJoin(joins);
		fileMeta.setPixelString(this.originalSignature);
		fileMeta.setType(FileMeta.FILE_TYPE.EXCEL);
		this.insight.addFileUsedInInsight(fileMeta);
	}
	
	/**
	 * Flush the task data into an array
	 * This assumes you have table data!!!
	 * @param taskData
	 * @return
	 */
	private List<Object> flushJobData(ITask taskData) {
		List<Object> flushedOutCol = new ArrayList<Object>();
		// iterate through the task to get the table data
		List<Object[]> data = taskData.flushOutIteratorAsGrid();
		int size = data.size();
		// assumes we are only flushing out the first column
		for(int i = 0; i < size; i++) {
			flushedOutCol.add(data.get(i)[0]);
		}
		
		return flushedOutCol;
	}
}