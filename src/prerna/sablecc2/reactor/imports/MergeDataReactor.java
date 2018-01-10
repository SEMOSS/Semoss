package prerna.sablecc2.reactor.imports;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Vector;

import org.apache.log4j.Logger;

import prerna.algorithm.api.ITableDataFrame;
import prerna.algorithm.api.SemossDataType;
import prerna.ds.OwlTemporalEngineMeta;
import prerna.engine.api.IHeadersDataRow;
import prerna.query.querystruct.CsvQueryStruct;
import prerna.query.querystruct.ExcelQueryStruct;
import prerna.query.querystruct.QueryStruct2;
import prerna.query.querystruct.filters.SimpleQueryFilter;
import prerna.query.querystruct.selectors.IQuerySelector;
import prerna.query.querystruct.selectors.QueryColumnSelector;
import prerna.sablecc2.om.GenRowStruct;
import prerna.sablecc2.om.Join;
import prerna.sablecc2.om.NounMetadata;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.reactor.AbstractReactor;
import prerna.util.ga.GATracker;

public class MergeDataReactor extends AbstractReactor {
	
	public MergeDataReactor() {
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
		QueryStruct2 qs = getQueryStruct();
		List<Join> joins = this.curRow.getAllJoins();
		// first convert the join to use the physical frame name in the selector
		joins = convertJoins(joins, frame.getMetaData());

		// track GA data
		GATracker.getInstance().trackDataImport(this.insight, qs);
		
		// if we have an inner join, add the current values as a filter on the query
		// important for performance on large dbs when the user has already 
		// filtered to small subset
		for(Join j : joins) {
			// s is the frame name
			String s = j.getSelector();
			// q is part of the query we are merging
			String q = j.getQualifier();
			String type = j.getJoinType();
			if(type.equals("inner.join") || type.equals("left.outer.join")) {
				// we need to make sure we apply the filter correctly!
				// remember, q is the alias we provide the selector
				// but might not match the physical
				if(!qs.hasColumn(q)) {
					IQuerySelector selector = qs.findSelectorFromAlias(q);
					// get the correct q
					q = selector.getQueryStructName();
				}
				// we will add a filter frame existing values in frame
				// but wait... need to make sure an existing filter isn't there
				if(qs.hasFiltered(q)) {
					continue;
				}
				QueryStruct2 filterQs = new QueryStruct2();
				QueryColumnSelector column = new QueryColumnSelector(s);
				filterQs.addSelector(column);
				try {
					Iterator<IHeadersDataRow> it = frame.query(filterQs);
					List<Object> values = new ArrayList<Object>();
					while(it.hasNext()) {
						values.add(it.next().getValues()[0]);
					}
					// create a selector
					// just set the table to be the alias
					// the frame will auto convert to physical
					QueryColumnSelector qSelector = new QueryColumnSelector();
					qSelector.setTable(q);
					qSelector.setColumn(QueryStruct2.PRIM_KEY_PLACEHOLDER);
					NounMetadata lNoun = new NounMetadata(qSelector, PixelDataType.COLUMN);
					NounMetadata rNoun = null;
					if(frame.getMetaData().getHeaderTypeAsEnum(s) == SemossDataType.NUMBER) {
						rNoun = new NounMetadata(values, PixelDataType.CONST_DECIMAL);
					} else {
						rNoun = new NounMetadata(values, PixelDataType.CONST_STRING);
					}
					SimpleQueryFilter filter = new SimpleQueryFilter(lNoun, "==", rNoun);
					qs.addFilter(filter);
				} catch(Exception e) {
					throw new IllegalArgumentException("Trying to merge on a column that does not exist within the frame!");
				}
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
	
	private ITableDataFrame getFrame() {
		// try specific key
		GenRowStruct frameGrs = this.store.getNoun(keysToGet[0]);
		if(frameGrs != null && !frameGrs.isEmpty()) {
			return (ITableDataFrame) frameGrs.get(0);
		}
		
		List<NounMetadata> frameCur = this.curRow.getNounsOfType(PixelDataType.FRAME);
		if(frameCur != null && !frameCur.isEmpty()) {
			return (ITableDataFrame) frameCur.get(0).getValue();
		}
		
		return (ITableDataFrame) this.insight.getDataMaker();
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