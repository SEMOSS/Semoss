package prerna.reactor.imports;

import java.io.File;
import java.io.IOException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Vector;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import prerna.algorithm.api.ITableDataFrame;
import prerna.algorithm.api.SemossDataType;
import prerna.ds.OwlTemporalEngineMeta;
import prerna.ds.TinkerFrame;
import prerna.ds.nativeframe.NativeFrame;
import prerna.engine.api.IDatabaseEngine;
import prerna.engine.api.IHeadersDataRow;
import prerna.engine.api.IRDBMSEngine;
import prerna.engine.api.IRawSelectWrapper;
import prerna.om.Insight;
import prerna.om.InsightFile;
import prerna.query.querystruct.AbstractQueryStruct;
import prerna.query.querystruct.AbstractQueryStruct.QUERY_STRUCT_TYPE;
import prerna.query.querystruct.CsvQueryStruct;
import prerna.query.querystruct.ExcelQueryStruct;
import prerna.query.querystruct.HardSelectQueryStruct;
import prerna.query.querystruct.LambdaQueryStruct;
import prerna.query.querystruct.SQLQueryUtils;
import prerna.query.querystruct.SelectQueryStruct;
import prerna.query.querystruct.filters.IQueryFilter;
import prerna.query.querystruct.filters.SimpleQueryFilter;
import prerna.query.querystruct.selectors.IQuerySelector;
import prerna.query.querystruct.selectors.QueryColumnSelector;
import prerna.query.querystruct.transform.QSAliasToPhysicalConverter;
import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.GenRowStruct;
import prerna.sablecc2.om.Join;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.execptions.SemossPixelException;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.sablecc2.om.task.ITask;
import prerna.util.Constants;
import prerna.util.usertracking.UserTrackerFactory;

public class MergeReactor extends AbstractReactor {
	
	private static final Logger classLogger = LogManager.getLogger(MergeReactor.class);
	
	public MergeReactor() {
		this.keysToGet = new String[]{ReactorKeysEnum.FRAME.getKey(), ReactorKeysEnum.QUERY_STRUCT.getKey(), ReactorKeysEnum.JOINS.getKey()};
	}

	@Override
	public NounMetadata execute()  {
		ITableDataFrame frame = getFrame();
		SelectQueryStruct qs = getQueryStruct();
		if(qs != null) {
			AbstractQueryStruct.QUERY_STRUCT_TYPE type = qs.getQsType();
			if( (type == QUERY_STRUCT_TYPE.FRAME || type == QUERY_STRUCT_TYPE.RAW_FRAME_QUERY) && qs.getFrame() == null) {
				qs.setFrame(frame);
			}
		}
		// set the logger into the frame
		Logger logger = getLogger(frame.getClass().getName());
		frame.setLogger(logger);
		
		// first convert the join to use the physical frame name in the selector
		List<Join> joins = getJoins();
		joins = convertJoins(joins, frame.getMetaData());
		
		// we could either be merging from a QS that we want to convert into a task
		// or it is a task already and we want to merge
		// in either case, we will not return anything but just update the frame
		
		
		// btw this can also be valid for HardQueryStruct any frame possibly.. 
		ITableDataFrame mergeFrame = null;
		
		ITableDataFrame curFrame = this.insight.getCurFrame();

		if(frame instanceof NativeFrame) {
			try {
				mergeFrame = mergeNative(curFrame, frame, qs, joins);
			} catch (Exception e) {
				classLogger.error(Constants.STACKTRACE, e);
				throw new SemossPixelException(e.getMessage(), e);
			}
		} 
		// did the merge go through on native ? if not
		if(mergeFrame == null)
		{
			if(qs != null) {
				try {
					mergeFrame = mergeFromQs(frame, qs, joins);
				} catch (Exception e) {
					classLogger.error(Constants.STACKTRACE, e);
					throw new SemossPixelException(e.getMessage(), e);
				}
			} else {
				ITask task = getTask();
				if(task != null) {
					try {
						mergeFrame = mergeFromTask(frame, task, joins);
					} catch (Exception e) {
						classLogger.error(Constants.STACKTRACE, e);
						throw new SemossPixelException(e.getMessage(), e);
					} finally {
						try {
							task.close();
						} catch (IOException e) {
							classLogger.error(Constants.STACKTRACE, e);
						}
					}
				} else {
					throw new IllegalArgumentException("Could not find any data input to merge into the frame");
				}
			}
		}		
		// clear cached info after merge
		mergeFrame.clearCachedMetrics();
		mergeFrame.clearQueryCache();

		NounMetadata noun = new NounMetadata(mergeFrame, PixelDataType.FRAME, PixelOperationType.FRAME_DATA_CHANGE, PixelOperationType.FRAME_HEADERS_CHANGE);
		// in case we generated a new frame
		// update existing references
		if(mergeFrame != frame) {
			if(frame.getName() != null) {
				this.insight.getVarStore().put(frame.getName(), noun);
			} 
			if(frame == this.insight.getVarStore().get(Insight.CUR_FRAME_KEY).getValue()) {
				this.insight.setDataMaker(mergeFrame);
			}
		}
		
		return noun;
	}
	
	private ITableDataFrame mergeNative(ITableDataFrame curFrame, ITableDataFrame frame, SelectQueryStruct qs, List<Join> joins) throws Exception {
		// track GA data
		UserTrackerFactory.getInstance().trackDataImport(this.insight, qs);
		
		/*
		/// OLD METHOD
		IImporter importer = ImportFactory.getImporter(frame, qs);
		// we reassign the frame because it might have changed
		// this only happens for native frame
		frame = importer.mergeData(joins);
		return frame;
		*/
		// get the database information of the current frame
		// get the databsase information of the second frame
		// need a way to find if they both are referring to the same physical database.. but for now, we just match the app let us say
		// need to get the qs from the native frame
		// get the engine from the qs and engine id
		// get the schema of the engine - this is important because you could have 2 apps in which case we need to make sure they are going off the same conn url / schema
		// may be we just check the connection URL

		ITableDataFrame mergeFrame = null;

		if(curFrame instanceof NativeFrame && frame instanceof NativeFrame) {
			try {
				// get the querystruct
				SelectQueryStruct curQS = ((NativeFrame)curFrame).getQueryStruct();
				curQS = QSAliasToPhysicalConverter.getPhysicalQs(curQS, curFrame.getMetaData());

				qs = ((NativeFrame)qs.getFrame()).getQueryStruct();
				qs = QSAliasToPhysicalConverter.getPhysicalQs(qs, qs.getFrame().getMetaData());

				IDatabaseEngine curEngine = curQS.getEngine();
				IDatabaseEngine thisEngine = qs.getEngine();
				if(thisEngine == null)
					thisEngine = qs.retrieveQueryStructEngine();

				if(curEngine == null)
					curEngine = curQS.retrieveQueryStructEngine();

				// check to see they are RDBMS
				if(curEngine instanceof IRDBMSEngine && thisEngine instanceof IRDBMSEngine) {
					// get the url now
					// we get the url because the focus area can be an app too
					// this way we can be sure
					
					//TODO: need to account for closing connection created for connection pooling
					//TODO: need to account for closing connection created for connection pooling
					//TODO: need to account for closing connection created for connection pooling
					//TODO: need to account for closing connection created for connection pooling
					//TODO: need to account for closing connection created for connection pooling
					//TODO: need to account for closing connection created for connection pooling

					String curURL = ((IRDBMSEngine)curEngine).getConnectionMetadata().getURL();
					String thisURL = ((IRDBMSEngine)thisEngine).getConnectionMetadata().getURL();


					if(curURL.equalsIgnoreCase(thisURL)) {
						// ok great these are same database
						// create the SQL Queries
						// need to check if these are query structs also
						mergeFrame = (NativeFrame) SQLQueryUtils.joinQueryStructs(curQS, qs, joins);
						mergeFrame.setName(curFrame.getName());
					} else {
						throw new SemossPixelException("Joining tables across databases is not possible, please consider converting to a materialized frame");
					}
				} else {
					throw new SemossPixelException("Joining to a native frame from a materialized frame not possible, please consider swapping the join order");

				}
			} catch (SQLException e) {
				classLogger.error(Constants.STACKTRACE, e);
			}
		}
		
		return mergeFrame;
	}

	/**
	 * Merge via a QS that we will execute into an iterator
	 * @param frame
	 * @param qs
	 * @param joins
	 * @return
	 * @throws Exception 
	 */
	private ITableDataFrame mergeFromQs(ITableDataFrame frame, SelectQueryStruct qs, List<Join> joins) throws Exception {
		// track GA data
		UserTrackerFactory.getInstance().trackDataImport(this.insight, qs);

		// if we have an inner join, add the current values as a filter on the query
		// important for performance on large dbs when the user has already 
		// filtered to small subset
		boolean noDataError = false;
		try {
			if(!(qs instanceof HardSelectQueryStruct)) {
				for(Join j : joins) {
					// the join format is
					// LHS = COLUMN NAME OF THE FRAME I AM MERGING INTO 
					// RHS = COLUMN NAME OF THE NEW DATA WE ARE JOINING TO
					// LHS IS WHAT IS MAINTAINED AFTER THE JOIN
					// RHS IS THE NAME IN THE QUERY
					String leftColumnJoin = j.getLColumn();
					String rColumnJoin = j.getRColumn();
					String type = j.getJoinType();
					String comparator = j.getComparator();
					
					if(IQueryFilter.comparatorIsEquals(comparator) && (type.equals("inner.join") || type.equals("left.outer.join"))) {
						// we need to make sure we apply the filter correctly!
						// remember, RHS is the alias we provide the selector
						// but might not match the physical
						if(!qs.hasColumn(rColumnJoin)) {
							IQuerySelector selector = null;
							if(rColumnJoin.contains("__")) {
								selector = qs.findSelectorFromAlias(rColumnJoin.split("__")[1]);
							} else {
								selector = qs.findSelectorFromAlias(rColumnJoin);
							}
							// get the correct q
							if(selector == null) {
								throw new IllegalArgumentException("There is an error with the join. Please make sure the columns are matched appropriately based on the frame you want to maintain");
							}
							rColumnJoin = selector.getQueryStructName();
						}
						// we will add a filter frame existing values in frame
						// but wait... need to make sure an existing filter isn't there
						if(qs.hasFiltered(rColumnJoin)) {
							continue;
						}
						
						// if current frame is empty
						// well, you will end up with no data
						// unless you are on a graph, which will just append nodes
						// as there is no real concept of joins currently
						if(frame.isEmpty()) {
							noDataError = true;
							throw new IllegalArgumentException("Attempting to join new data with an empty frame. End result is still an empty frame.");
						}
						
						SelectQueryStruct filterQs = new SelectQueryStruct();
						QueryColumnSelector column = new QueryColumnSelector(leftColumnJoin);
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
							
							PixelDataType dataType = PixelDataType.CONST_STRING;
							SemossDataType sDataType = frame.getMetaData().getHeaderTypeAsEnum(leftColumnJoin);
							if(sDataType == SemossDataType.INT) {
								dataType = PixelDataType.CONST_INT;
							} else if(sDataType == SemossDataType.DOUBLE) {
								dataType = PixelDataType.CONST_DECIMAL;
							}
							
							qs.addImplicitFilter(SimpleQueryFilter.makeColToValFilter(rColumnJoin, "==", values, dataType));
						} catch(Exception e) {
							classLogger.error(Constants.STACKTRACE, e);
							throw new IllegalArgumentException("Trying to merge on a column that does not exist within the frame!");
						}
					}
				}
			}
		} catch(IllegalArgumentException e) {
			if(!noDataError) {
				throw e;
			}
		}
		
		// i already know
		// that the current frame has no data
		// this will return nothing when we attempt to do the join
		// so add limit of 1
		// adding exception for tinker since we never actually do 
		// join types and everything is an outer
		if(noDataError && !(frame instanceof TinkerFrame) ) {
			qs.setLimit(1);
			classLogger.info("Adjusting merge query with limit 1 because frame is empty");
		}
		
		IRawSelectWrapper it = null;
		try {
			it = ImportUtility.generateIterator(qs, frame);
			if(!FrameSizeRetrictions.importWithinLimit(frame, it)) {
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
		} finally {
			if(it != null) {
				try {
					it.close();
				} catch(IOException e) {
					classLogger.error(Constants.STACKTRACE, e);
				}
			}
		}
	}
	
	/**
	 * Merge via a Task
	 * @param frame
	 * @param task
	 * @param joins
	 * @throws Exception 
	 */
	private ITableDataFrame mergeFromTask(ITableDataFrame frame, ITask task, List<Join> joins) throws Exception {
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
			String origLCol = j.getLColumn();
			String newLCol = meta.getUniqueNameFromAlias(origLCol);
			if(newLCol == null) {
				// nothing to do
				// add the original back
				convertedJoins.add(j);
				continue;
			}
			// or an alias was used
			// so make a new Join and add it to the list
			Join newJ = new Join(newLCol, j.getJoinType(), j.getRColumn(), j.getComparator(), j.getJoinRelName());
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
			InsightFile insightFile = new InsightFile();
			insightFile.setFilePath(qs.getFilePath());
			insightFile.setFrameUpload(true);
			this.insight.addLoadInsightFile(insightFile);
		} else {
			// it is from an API call of some sort
			// delete it
			// when we save, we want to repull every time
			File csvFile = new File(qs.getFilePath());
			csvFile.delete();
		}
	}
	
	private void storeExcelFileMeta(ExcelQueryStruct qs, List<Join> joins) {
		InsightFile insightFile = new InsightFile();
		insightFile.setFilePath(qs.getFilePath());
		insightFile.setFrameUpload(true);
		this.insight.addLoadInsightFile(insightFile);
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
		
		ITableDataFrame defaultFrame = (ITableDataFrame) this.insight.getDataMaker();
		this.store.makeNoun(ReactorKeysEnum.FRAME.getKey()).add(new NounMetadata(defaultFrame, PixelDataType.FRAME));
		return defaultFrame;
	}
	
	protected SelectQueryStruct getQueryStruct() {
		SelectQueryStruct queryStruct = null;

		GenRowStruct grs = this.store.getNoun(this.keysToGet[1]);
		if(grs != null) {
			NounMetadata object = (NounMetadata)grs.getNoun(0);
			return (SelectQueryStruct)object.getValue();
		}
		
		grs = this.store.getNoun(PixelDataType.QUERY_STRUCT.getKey());
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
		GenRowStruct allNouns = getNounStore().getNoun(PixelDataType.TASK.getKey());
		ITask task = null;
		if(allNouns != null) {
			NounMetadata object = (NounMetadata)allNouns.getNoun(0);
			return (ITask)object.getValue();
		} 

		return task;
	}
	
	public String getName()
	{
		return "Merge";
	}

}