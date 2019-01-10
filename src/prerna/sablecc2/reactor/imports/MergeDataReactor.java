package prerna.sablecc2.reactor.imports;

import java.io.File;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Vector;

import org.apache.log4j.Logger;

import prerna.algorithm.api.ITableDataFrame;
import prerna.algorithm.api.SemossDataType;
import prerna.ds.OwlTemporalEngineMeta;
import prerna.ds.nativeframe.NativeFrame;
import prerna.engine.api.IHeadersDataRow;
import prerna.engine.api.IRawSelectWrapper;
import prerna.om.Insight;
import prerna.query.querystruct.CsvQueryStruct;
import prerna.query.querystruct.ExcelQueryStruct;
import prerna.query.querystruct.HardSelectQueryStruct;
import prerna.query.querystruct.LambdaQueryStruct;
import prerna.query.querystruct.SelectQueryStruct;
import prerna.query.querystruct.filters.SimpleQueryFilter;
import prerna.query.querystruct.selectors.IQuerySelector;
import prerna.query.querystruct.selectors.QueryColumnSelector;
import prerna.sablecc2.om.GenRowStruct;
import prerna.sablecc2.om.Join;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.execptions.SemossPixelException;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.sablecc2.om.task.ITask;
import prerna.sablecc2.reactor.AbstractReactor;
import prerna.util.usertracking.UserTrackerFactory;

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
		
		// first convert the join to use the physical frame name in the selector
		List<Join> joins = getJoins();
		joins = convertJoins(joins, frame.getMetaData());
		
		// we could either be merging from a QS that we want to convert into a task
		// or it is a task already and we want to merge
		// in either case, we will not return anything but just update the frame
		
		ITableDataFrame mergeFrame = null;
		SelectQueryStruct qs = getQueryStruct();
		if(frame instanceof NativeFrame) {
			mergeFrame = mergeNative(frame, qs, joins);
		} else if(qs != null) {
			mergeFrame = mergeFromQs(frame, qs, joins);
		} else {
			ITask task = getTask();
			if(task != null) {
				mergeFrame = mergeFromTask(frame, task, joins);
			} else {
				throw new IllegalArgumentException("Could not find any data input to merge into the frame");
			}
		}
		
		// clear cached info after merge
		frame.clearCachedInfo();

		NounMetadata noun = new NounMetadata(mergeFrame, PixelDataType.FRAME, PixelOperationType.FRAME_DATA_CHANGE, PixelOperationType.FRAME_HEADERS_CHANGE);
		noun.addAdditionalReturn(new NounMetadata(mergeFrame.getMetaData().getTableHeaderObjects(), PixelDataType.CUSTOM_DATA_STRUCTURE, PixelOperationType.FRAME_HEADERS));
		// in case we generated a new frame
		// update existing references
		if(mergeFrame != frame) {
			if(frame.getTableName() != null) {
				this.insight.getVarStore().put(frame.getTableName(), noun);
			} 
			if(frame == this.insight.getVarStore().get(Insight.CUR_FRAME_KEY).getValue()) {
				this.insight.setDataMaker(mergeFrame);
			}
		}
		
		return noun;
	}
	
	private ITableDataFrame mergeNative(ITableDataFrame frame, SelectQueryStruct qs, List<Join> joins) {
		// track GA data
//		UserTrackerFactory.getInstance().trackDataImport(this.insight, qs);
		UserTrackerFactory.getInstance().trackDataImport(this.insight, qs);

		IImporter importer = ImportFactory.getImporter(frame, qs);
		// we reassign the frame because it might have changed
		// this only happens for native frame
		frame = importer.mergeData(joins);
		return frame;
	}

	/**
	 * Merge via a QS that we will execute into an iterator
	 * @param frame
	 * @param qs
	 * @param joins
	 * @return
	 */
	private ITableDataFrame mergeFromQs(ITableDataFrame frame, SelectQueryStruct qs, List<Join> joins) {
		// track GA data
//		UserTrackerFactory.getInstance().trackDataImport(this.insight, qs);
		UserTrackerFactory.getInstance().trackDataImport(this.insight, qs);

		// if we have an inner join, add the current values as a filter on the query
		// important for performance on large dbs when the user has already 
		// filtered to small subset
		if(!(qs instanceof HardSelectQueryStruct)) {
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
					SelectQueryStruct filterQs = new SelectQueryStruct();
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
						QueryColumnSelector qSelector = new QueryColumnSelector(q);
						NounMetadata lNoun = new NounMetadata(qSelector, PixelDataType.COLUMN);
						NounMetadata rNoun = null;
						SemossDataType dataType = frame.getMetaData().getHeaderTypeAsEnum(s);
						if(dataType == SemossDataType.INT) {
							rNoun = new NounMetadata(values, PixelDataType.CONST_INT);
						} else if(dataType == SemossDataType.DOUBLE) {
							rNoun = new NounMetadata(values, PixelDataType.CONST_DECIMAL);
						} else {
							rNoun = new NounMetadata(values, PixelDataType.CONST_STRING);
						}
						SimpleQueryFilter filter = new SimpleQueryFilter(lNoun, "==", rNoun);
						qs.addImplicitFilter(filter);
					} catch(Exception e) {
						e.printStackTrace();
						throw new IllegalArgumentException("Trying to merge on a column that does not exist within the frame!");
					}
				}
			}
		}
		
		IRawSelectWrapper it = null;
		try {
			it = ImportUtility.generateIterator(qs, frame);
		} catch (Exception e) {
			e.printStackTrace();
			throw new SemossPixelException(
					new NounMetadata("Error occured executing query before loading into frame", 
							PixelDataType.CONST_STRING, PixelOperationType.ERROR));
		}
		if(!ImportSizeRetrictions.mergeWithinLimit(frame, it)) {
			SemossPixelException exception = new SemossPixelException(
					new NounMetadata("Frame size is too large, please limit the data size before proceeding", 
							PixelDataType.CONST_STRING, 
							PixelOperationType.FRAME_SIZE_LIMIT_EXCEEDED, PixelOperationType.ERROR));
			exception.setContinueThreadOfExecution(false);
			throw exception;
		}
		
		IImporter importer = ImportFactory.getImporter(frame, qs, it);
		// we reassign the frame because it might have changed
		// this only happens for native frame
		frame = importer.mergeData(joins);
		
		if(qs.getQsType() == SelectQueryStruct.QUERY_STRUCT_TYPE.CSV_FILE) {
			storeCsvFileMeta((CsvQueryStruct) qs, this.curRow.getAllJoins());
		} else if(qs.getQsType() == SelectQueryStruct.QUERY_STRUCT_TYPE.EXCEL_FILE) {
			storeExcelFileMeta((ExcelQueryStruct) qs, this.curRow.getAllJoins());
		}
		
		return frame;
	}
	
	/**
	 * Merge via a Task
	 * @param frame
	 * @param task
	 * @param joins
	 */
	private ITableDataFrame mergeFromTask(ITableDataFrame frame, ITask task, List<Join> joins) {
		LambdaQueryStruct qs = new LambdaQueryStruct();
		
		// go through the metadata on the task
		// and add it to the qs
		Map<String, String> dataTypes = new HashMap<String, String>();
		List<Map<String, Object>> taskHeaders = task.getHeaderInfo();
		for(Map<String, Object> headerInfo : taskHeaders) {
			String alias = (String) headerInfo.get("alias");
			String type = (String) headerInfo.get("type");
			qs.addSelector(new QueryColumnSelector(alias));
			dataTypes.put(alias, type);
		}
		qs.setColumnTypes(dataTypes);
		
		IImporter importer = ImportFactory.getImporter(frame, qs, task);
		// we reassign the frame because it might have changed
		// this only happens for native frame
		frame = importer.mergeData(joins);
		
		return frame;
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
	
	///////////////////////////////////////////////////////////////////////
	///////////////////////////////////////////////////////////////////////
	///////////////////////////////////////////////////////////////////////
	///////////////////////////////////////////////////////////////////////
	
	/*
	 * Store the file if used
	 */
	
	private void storeCsvFileMeta(CsvQueryStruct qs, List<Join> joins) {
		if(qs.getSource() == CsvQueryStruct.ORIG_SOURCE.FILE_UPLOAD) {
			FileMeta fileMeta = new FileMeta();
			fileMeta.setFileLoc(qs.getFilePath());
			fileMeta.setDataMap(qs.getColumnTypes());
			fileMeta.setNewHeaders(qs.getNewHeaderNames());
			fileMeta.setPixelString(this.originalSignature);
			fileMeta.setSelectors(qs.getSelectors());
			fileMeta.setType(FileMeta.FILE_TYPE.CSV);
			this.insight.addFileUsedInInsight(fileMeta);
		} else {
			// it is from an API call of some sort
			// delete it
			// when we save, we want to repull every time
			File csvFile = new File(qs.getFilePath());
			csvFile.delete();
		}
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
	
	///////////////////////////////////////////////////////////////////////
	///////////////////////////////////////////////////////////////////////
	///////////////////////////////////////////////////////////////////////
	///////////////////////////////////////////////////////////////////////
	
	/*
	 * Getters for the reactor
	 */
	
	protected ITableDataFrame getFrame() {
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
	
	protected SelectQueryStruct getQueryStruct() {
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
	
	protected List<Join> getJoins() {
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

	private ITask getTask() {
		GenRowStruct allNouns = getNounStore().getNoun(PixelDataType.TASK.name());
		ITask task = null;
		if(allNouns != null) {
			NounMetadata object = (NounMetadata)allNouns.getNoun(0);
			return (ITask)object.getValue();
		} 

		return task;
	}
}