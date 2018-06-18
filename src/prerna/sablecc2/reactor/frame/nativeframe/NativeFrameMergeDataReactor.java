package prerna.sablecc2.reactor.frame.nativeframe;

import java.util.List;
import java.util.Vector;

import org.apache.log4j.Logger;

import prerna.algorithm.api.ITableDataFrame;
import prerna.ds.OwlTemporalEngineMeta;
import prerna.query.querystruct.CsvQueryStruct;
import prerna.query.querystruct.ExcelQueryStruct;
import prerna.query.querystruct.SelectQueryStruct;
import prerna.query.querystruct.selectors.IQuerySelector;
import prerna.query.querystruct.selectors.QueryColumnSelector;
import prerna.sablecc2.om.GenRowStruct;
import prerna.sablecc2.om.Join;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.sablecc2.reactor.AbstractReactor;
import prerna.sablecc2.reactor.imports.FileMeta;
import prerna.sablecc2.reactor.imports.IImporter;
import prerna.sablecc2.reactor.imports.ImportFactory;
import prerna.util.ga.GATracker;

public class NativeFrameMergeDataReactor extends AbstractReactor {

	public NativeFrameMergeDataReactor() {
		this.keysToGet = new String[]{ReactorKeysEnum.FRAME.getKey(), ReactorKeysEnum.QUERY_STRUCT.getKey(), ReactorKeysEnum.JOINS.getKey()};
	}
	
	@Override
	public NounMetadata execute()  {
		ITableDataFrame frame = getFrame();
		// set the logger into the frame
		Logger logger = getLogger(frame.getClass().getName());
		frame.setLogger(logger);
		
		// this is greedy execution
		// will not return anything
		// but will update the frame in the pixel planner
		SelectQueryStruct qs = getQueryStruct();
		List<Join> joins = getJoins();
		// first convert the join to use the physical frame name in the selector
		joins = convertJoins(joins, frame.getMetaData());

		// track GA data
		GATracker.getInstance().trackDataImport(this.insight, qs);

		// Format and send Google Analytics data
		String engine = qs.getEngineId() + "";
		// if the engine doesnt have a name then the data is coming from a temp table
		if (qs.getEngineId() == null){
			String tempFrameName = qs.getFrame() + "";
			engine = "TempFrame_" + tempFrameName ;
		}
		String curExpression = "";
		List<IQuerySelector> selectors = qs.getSelectors();
		for (int i = 0; i < selectors.size(); i++) {
			IQuerySelector selector = selectors.get(i);
			String columnSelected = "";
			if (selector instanceof QueryColumnSelector) {
				// we can get a table and column
				columnSelected = ((QueryColumnSelector) selector).getTable() + "__" + ((QueryColumnSelector) selector).getAlias();
			} else {
				// only alias
				columnSelected = selector.getAlias();
			}
			curExpression = curExpression + engine + ":" + columnSelected;
			if (i != (selectors.size() - 1)) {
				curExpression += ";";
			}
		}
		if (curExpression.equals("") && engine.equals("DIRECT_ENGINE_CONNECTION")){
			curExpression = curExpression + engine ;
		}
		insight.trackPixels("dataquery", curExpression);
		
		IImporter importer = ImportFactory.getImporter(frame, qs);
		// we reassign the frame because it might have changed
		// this only happens for native frame
		frame = importer.mergeData(joins);
		this.insight.setDataMaker(frame);
		// need to clear the unique col count used by FE for determining the need for math
		frame.clearCachedInfo();
		if(qs.getQsType() == SelectQueryStruct.QUERY_STRUCT_TYPE.CSV_FILE) {
			storeCsvFileMeta((CsvQueryStruct) qs, this.curRow.getAllJoins());
		} else if(qs.getQsType() == SelectQueryStruct.QUERY_STRUCT_TYPE.EXCEL_FILE) {
			storeExcelFileMeta((ExcelQueryStruct) qs, this.curRow.getAllJoins());
		}
		
		NounMetadata noun = new NounMetadata(frame, PixelDataType.FRAME, PixelOperationType.FRAME_DATA_CHANGE, PixelOperationType.FRAME_HEADERS_CHANGE);
		noun.addAdditionalReturn(new NounMetadata(frame.getMetaData().getTableHeaderObjects(), PixelDataType.CUSTOM_DATA_STRUCTURE, PixelOperationType.FRAME_HEADERS));
		return noun;
	}
	
	///////////////////////////////////////////////////////////////////////
	///////////////////////////////////////////////////////////////////////
	///////////////////////////////////////////////////////////////////////
	///////////////////////////////////////////////////////////////////////
	
	/*
	 * Getters for the reactor
	 */
	
	private ITableDataFrame getFrame() {
		// try specific key
		GenRowStruct frameGrs = this.store.getNoun(this.keysToGet[0]);
		if(frameGrs != null && !frameGrs.isEmpty()) {
			return (ITableDataFrame) frameGrs.get(0);
		}
		
		List<NounMetadata> frameCur = this.curRow.getNounsOfType(PixelDataType.FRAME);
		if(frameCur != null && !frameCur.isEmpty()) {
			return (ITableDataFrame) frameCur.get(0).getValue();
		}
		
		return (ITableDataFrame) this.insight.getDataMaker();
	}
	
	private SelectQueryStruct getQueryStruct() {
		SelectQueryStruct queryStruct = null;

		GenRowStruct grs = this.store.getNoun(this.keysToGet[1]);
		if(grs != null) {
			NounMetadata object = (NounMetadata)grs.getNoun(0);
			return (SelectQueryStruct)object.getValue();
		}
		
		grs = this.store.getNoun(PixelDataType.QUERY_STRUCT.toString());
		if(grs != null) {
			NounMetadata object = (NounMetadata) grs.getNoun(0);
			return (SelectQueryStruct)object.getValue();
		}

		return queryStruct;
	}
	
	private List<Join> getJoins() {
		List<Join> joins = new Vector<Join>();
		// try specific key
		{
			GenRowStruct grs = this.store.getNoun(this.keysToGet[2]);
			if(grs != null && !grs.isEmpty()) {
				joins = grs.getAllJoins();
				if(joins != null && !joins.isEmpty()) {
					return joins;
				}
			}
		}
		
		List<NounMetadata> joinsCur = this.curRow.getNounsOfType(PixelDataType.JOIN);
		if(joinsCur != null && !joinsCur.isEmpty()) {
			int size = joinsCur.size();
			for(int i = 0; i < size; i++) {
				joins.add( (Join) joinsCur.get(i).getValue());
			}
			
			return joins;
		}
		
		throw new IllegalArgumentException("Could not find the columns for the join");
	}

	private void storeCsvFileMeta(CsvQueryStruct qs, List<Join> joins) {
		FileMeta fileMeta = new FileMeta();
		fileMeta.setFileLoc(qs.getFilePath());
		fileMeta.setDataMap(qs.getColumnTypes());
		fileMeta.setNewHeaders(qs.getNewHeaderNames());
		fileMeta.setPixelString(this.originalSignature);
		fileMeta.setSelectors(qs.getSelectors());
		fileMeta.setType(FileMeta.FILE_TYPE.CSV);
		this.insight.addFileUsedInInsight(fileMeta);
	}
	
	private void storeExcelFileMeta(ExcelQueryStruct qs, List<Join> joins) {
		FileMeta fileMeta = new FileMeta();
		fileMeta.setFileLoc(qs.getFilePath());
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
	 * Convert the frame join name from the alias to the physical table__column name
	 * @param joins
	 * @param meta
	 * @return
	 */
	private List<Join> convertJoins(List<Join> joins, OwlTemporalEngineMeta meta) {
		List<Join> convertedJoins = new Vector<Join>();
		for(Join j : joins) {
			String origLCol = j.getSelector();
			String newLCol = meta.getUniqueNameFromAlias(origLCol);
			if(newLCol == null) {
				// nothing to do
				// add the original back
				convertedJoins.add(j);
				continue;
			}
			// or an alias was used
			// so make a new Join and add it to the list
			Join newJ = new Join(newLCol, j.getJoinType(), j.getQualifier(), j.getJoinRelName());
			convertedJoins.add(newJ);
		}
		
		
		return convertedJoins;
	}
}