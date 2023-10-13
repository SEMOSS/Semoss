package prerna.reactor.imports;

import java.io.File;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Vector;

import org.apache.logging.log4j.Logger;

import prerna.algorithm.api.ITableDataFrame;
import prerna.algorithm.api.SemossDataType;
import prerna.ds.OwlTemporalEngineMeta;
import prerna.ds.TinkerFrame;
import prerna.ds.nativeframe.NativeFrame;
import prerna.ds.py.PandasFrame;
import prerna.ds.r.RDataTable;
import prerna.ds.r.RSyntaxHelper;
import prerna.engine.api.IHeadersDataRow;
import prerna.engine.api.IRawSelectWrapper;
import prerna.om.Insight;
import prerna.om.InsightFile;
import prerna.query.querystruct.AbstractQueryStruct;
import prerna.query.querystruct.CsvQueryStruct;
import prerna.query.querystruct.ExcelQueryStruct;
import prerna.query.querystruct.SelectQueryStruct;
import prerna.query.querystruct.filters.SimpleQueryFilter;
import prerna.query.querystruct.selectors.IQuerySelector;
import prerna.query.querystruct.selectors.QueryColumnSelector;
import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.GenRowStruct;
import prerna.sablecc2.om.Join;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.execptions.SemossPixelException;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.usertracking.UserTrackerFactory;

public class MergeFramesReactor extends AbstractReactor {
	
	private static final String sourceFrame = "source";
	private static final String targetFrame = "target";
	private static final String sourceCols = "sourceCols";
	private static final String targetCols = "targetCols";
	private static final String joinType = "jType";

	private static final String CLASS_NAME = MergeFramesReactor.class.getName();
	
	public MergeFramesReactor() {
		this.keysToGet = new String[]{sourceFrame, targetFrame, sourceCols, targetCols, joinType}; 

	}

	@Override
	public NounMetadata execute()  {
		ITableDataFrame sourceFrame = getSourceFrame();
		ITableDataFrame targetFrame = getTargetFrame();
		
		if(sourceFrame.isEmpty() || targetFrame.isEmpty()) {
			throw new IllegalArgumentException("Attempting to merge with an empty frame");
		}
		
		// set the logger into the frames
		Logger logger = getLogger(CLASS_NAME);
		sourceFrame.setLogger(logger);
		targetFrame.setLogger(logger);

		// first convert the join to use the physical frame name in the selector
		List<Join> joins = getJoins();
		joins = convertJoins(joins, sourceFrame.getMetaData(), targetFrame.getMetaData());
		
		// let us try to optimize 
		// if the frames are the same type
		
		// 1) are both R frames
		// 2) are both Python frames
		// 3) are both Native frames and using the same engine
		// 4) have go through a task + CSV - if one is native, must flush out to another frame
		
		// TODO: need to add for tracking...
		boolean optimized = false;
		// they are the same type
		if(sourceFrame.getFrameType() == targetFrame.getFrameType()) {
			// 1) are they both R
			if(sourceFrame instanceof RDataTable) {
				optimized = true;
				
				String joinType = null;
				List<Map<String, String>> joinCols = new ArrayList<Map<String, String>>();
				for(Join joinItem : joins) {
					joinType = joinItem.getJoinType();
					// in R, the existing column is referenced as frame__column
					// but the R syntax only wants the col
					Map<String, String> joinColMapping = new HashMap<String, String>();
					String jLeftColumn = joinItem.getLColumn();
					if(jLeftColumn.contains("__")) {
						jLeftColumn = jLeftColumn.split("__")[1];
					}
					String jRightColumn = joinItem.getRColumn();
					if(jRightColumn.contains("__")) {
						jRightColumn = jRightColumn.split("__")[1];
					}
					joinColMapping.put(jLeftColumn, jRightColumn);
					joinCols.add(joinColMapping);
				}
				
				// few steps to perform within this
				// a) need to rename the columns if they exist in both source and target and not part of the join
				// b) need to perform the merge
				// c) need to update the metadata
				
				String sourceFrameName = sourceFrame.getName();
				
				Map<String, SemossDataType> leftTableTypes = targetFrame.getMetaData().getHeaderToTypeMap();
				Map<String, SemossDataType> rightTableTypes = sourceFrame.getMetaData().getHeaderToTypeMap();
				
				Set<String> leftTableHeaders = leftTableTypes.keySet();
				Set<String> rightTableHeaders = rightTableTypes.keySet();
				Set<String> rightTableJoinCols = AbstractImporter.getRightJoinColumns(joins);
				
				Map<String, String> rightTableAlias = new HashMap<String, String>();

				// note, we are not going to modify the existing headers
				// even though the query builder code allows for it
				for(String leftTableHeader : leftTableHeaders) {
					if(leftTableHeader.contains("__")) {
						leftTableHeader = leftTableHeader.split("__")[1];
					}
					// instead of making the method return a boolean and then having to perform
					// another ignore case match later on
					// we return the match and do a null check
					String dupRightTableHeader = AbstractImporter.setIgnoreCaseMatch(leftTableHeader, rightTableHeaders, rightTableJoinCols);
					if(dupRightTableHeader != null) {
						rightTableAlias.put(dupRightTableHeader, leftTableHeader + "_1");
					}
				}
				
				
				
				
				String mergeString = RSyntaxHelper.getMergeSyntax(targetFrame.getName(), targetFrame.getName(), sourceFrame.getName(),  joinType, joinCols);
				((RDataTable) targetFrame).executeRScript(mergeString);
				((RDataTable) targetFrame).recreateMeta();

			}
			
			// 2) are they both Py
			else if(sourceFrame instanceof PandasFrame) {
				optimized = true;
				
				String joinType = null;
				List<Map<String, String>> joinCols = new ArrayList<Map<String, String>>();
				List<String> joinComparators = new ArrayList<>();
				boolean nonEqui = false;
				for(Join joinItem : joins) {
					joinType = joinItem.getJoinType();
					// in R, the existing column is referenced as frame__column
					// but the R syntax only wants the col
					Map<String, String> joinColMapping = new HashMap<String, String>();
					String jLeftColumn = joinItem.getLColumn();
					if(jLeftColumn.contains("__")) {
						jLeftColumn = jLeftColumn.split("__")[1];
					}
					String jRightColumn = joinItem.getRColumn();
					if(jRightColumn.contains("__")) {
						jRightColumn = jRightColumn.split("__")[1];
					}
					joinColMapping.put(jLeftColumn, jRightColumn);
					joinCols.add(joinColMapping);
					String joinComparator = joinItem.getComparator();
					if (!joinComparator.equals("=")) {
						nonEqui = true;
					}
					joinComparators.add(joinComparator);
				}
				
				// few steps to perform within this
				// a) need to rename the columns if they exist in both source and target and not part of the join
				// b) need to perform the merge
				// c) need to update the metadata
				
				((PandasFrame) targetFrame).merge(targetFrame.getName(), targetFrame.getName(), sourceFrame.getName(), joinType, joinCols, joinComparators, nonEqui);
				((PandasFrame) targetFrame).recreateMeta();
				((PandasFrame) targetFrame).replaceWrapperDataFromFrame();

			}
			
			// 3) are they both native
			else if(sourceFrame instanceof NativeFrame) {
				// need to ensure they are the same engine
				NativeFrame sourceNFrame = (NativeFrame) sourceFrame;
				NativeFrame targetNFrame = (NativeFrame) targetFrame;
				if(sourceNFrame.getEngineId().equals(targetNFrame.getEngineId())) {
					
					SelectQueryStruct sourceQs = sourceNFrame.getQueryStruct();
					SelectQueryStruct targetQs = targetNFrame.getQueryStruct();
					
					// at the moment, cannot merge from 2 custom froms
					if(sourceQs.getCustomFrom() == null && targetQs.getCustomFrom() == null) {
						optimized = true;
						targetQs.merge(sourceQs);
						//targetQs.addRelation(fromConcept, toConcept, joinType);
						
						NativeImporter importer = (NativeImporter) ImportFactory.getImporter(targetNFrame, targetQs, null);
						// we reassign the frame because it might have changed
						// this only happens for native frame
						try {
							importer.appendNecessaryRels(joins);
							importer.insertData();
						} catch (Exception e) {
							// TODO Auto-generated catch block
							e.printStackTrace();
						}
					}
				}
			}
		}
		
		if(!optimized) {
			// we will query and flush this out ...
			SelectQueryStruct qs = sourceFrame.getMetaData().getFlatTableQs(true);
			qs.setFrame(sourceFrame);
			qs.setQsType(AbstractQueryStruct.QUERY_STRUCT_TYPE.FRAME);
			ITableDataFrame mergeFrame = null;
			if(targetFrame instanceof NativeFrame) {
				try {
					SelectQueryStruct nativeQs = ((NativeFrame)targetFrame).getQueryStruct();
					//the qs's frame on the target frame is the sourceFrame. Later on during the import process for 
					// taking this native frame to an R frame, we read the qs then get qs.getFrame to generate metadata
					// from frame.getMeta. this will break since it has the sourceFrame's meta.
					// unless we reset the qs's frame to the targetFrame, we won't hold the meta anywhere
					((NativeFrame) targetFrame).getQueryStruct().setFrame(targetFrame);
					mergeFrame = mergeNative(targetFrame, qs, joins);
				} catch (Exception e) {
					e.printStackTrace();
					throw new SemossPixelException(e.getMessage());
				}
			} else if(qs != null) {
				try {
					mergeFrame = mergeFromQs(targetFrame, qs, joins);
				} catch (Exception e) {
					e.printStackTrace();
					throw new SemossPixelException(e.getMessage());
				}
			}
			// clear cached info after merge
			targetFrame.clearCachedMetrics();
			targetFrame.clearQueryCache();
			
			NounMetadata noun = new NounMetadata(mergeFrame, PixelDataType.FRAME, PixelOperationType.FRAME_DATA_CHANGE, PixelOperationType.FRAME_HEADERS_CHANGE);
			// in case we generated a new frame
			// update existing references
			if(mergeFrame != targetFrame) {
				if(targetFrame.getName() != null) {
					this.insight.getVarStore().put(targetFrame.getName(), noun);
				} 
				if(targetFrame == this.insight.getVarStore().get(Insight.CUR_FRAME_KEY).getValue()) {
					this.insight.setDataMaker(mergeFrame);
				}
			}
			
			return noun;
		}
		
		// clear cached info after merge
		targetFrame.clearCachedMetrics();
		targetFrame.clearQueryCache();
		
		NounMetadata noun = new NounMetadata(targetFrame, PixelDataType.FRAME, PixelOperationType.FRAME_DATA_CHANGE, PixelOperationType.FRAME_HEADERS_CHANGE);
		return noun;
	}
	
	private ITableDataFrame mergeNative(ITableDataFrame frame, SelectQueryStruct qs, List<Join> joins) throws Exception {
		// track GA data
		UserTrackerFactory.getInstance().trackDataImport(this.insight, qs);

		IImporter importer = ImportFactory.getImporter(frame, qs);
		importer.setInsight(this.insight);
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
			for(Join j : joins) {
				// the join format is
				// LHS = COLUMN NAME OF THE FRAME I AM MERGING INTO 
				// RHS = COLUMN NAME OF THE NEW DATA WE ARE JOINING TO
				// LHS IS WHAT IS MAINTAINED AFTER THE JOIN
				// RHS IS THE NAME IN THE QUERY
				String leftColumnJoin = j.getLColumn();
				String rColumnJoin = j.getRColumn();
				String type = j.getJoinType();

				if(leftColumnJoin.contains("__")) {
					leftColumnJoin = leftColumnJoin.split("__")[1];
				}
				if(rColumnJoin.contains("__")) {
					rColumnJoin = rColumnJoin.split("__")[1];
				}
				
				if(type.equals("inner.join") || type.equals("left.outer.join")) {
					// we need to make sure we apply the filter correctly!
					// remember, RHS is the alias we provide the selector
					// but might not match the physical
					if(!qs.hasColumn(rColumnJoin)) {
						IQuerySelector selector = qs.findSelectorFromAlias(rColumnJoin);
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
						e.printStackTrace();
						throw new IllegalArgumentException("Trying to merge on a column that does not exist within the frame!");
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
		}
		
		IRawSelectWrapper it = null;
		try {
			it = ImportUtility.generateIterator(qs, frame);
		} catch (Exception e) {
			e.printStackTrace();
			throw new SemossPixelException(
					new NounMetadata("Error occurred executing query before loading into frame", 
							PixelDataType.CONST_STRING, PixelOperationType.ERROR));
		}
		try {
			if(!FrameSizeRetrictions.importWithinLimit(frame, it)) {
				SemossPixelException exception = new SemossPixelException(
						new NounMetadata("Frame size is too large, please limit the data size before proceeding", 
								PixelDataType.CONST_STRING, 
								PixelOperationType.FRAME_SIZE_LIMIT_EXCEEDED, PixelOperationType.ERROR));
				exception.setContinueThreadOfExecution(false);
				throw exception;
			}
		} catch (SemossPixelException e) {
			throw e;
		} catch (Exception e) {
			e.printStackTrace();
			throw new SemossPixelException(e.getMessage());
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
	 * Convert the frame join name from the alias to the physical table__column name
	 * @param joins
	 * @param sourceMeta
	 * @param targetMeta
	 * @return
	 */
	private List<Join> convertJoins(List<Join> joins, OwlTemporalEngineMeta sourceMeta, OwlTemporalEngineMeta targetMeta) {
		List<Join> convertedJoins = new Vector<Join>();
		for(Join j : joins) {
			String origLCol = j.getLColumn();
			String origRCol = j.getRColumn();
			String newLCol = targetMeta.getUniqueNameFromAlias(origLCol);
			String newRCol = sourceMeta.getUniqueNameFromAlias(origRCol);
			if(newLCol == null && newRCol == null) {
				// nothing to do
				// add the original back
				convertedJoins.add(j);
			} 
			// an alias was used - replace and make a new Join
			// and add it to the list
			else if(newLCol != null && newRCol == null) {
				Join newJ = new Join(newLCol, j.getJoinType(), j.getRColumn(), j.getComparator(), j.getJoinRelName());
				convertedJoins.add(newJ);
			} else if(newLCol == null && newRCol != null) {
				Join newJ = new Join(j.getLColumn(), j.getJoinType(), newRCol, j.getComparator(), j.getJoinRelName());
				convertedJoins.add(newJ);
			} else {
				Join newJ = new Join(newLCol, j.getJoinType(), newRCol, j.getComparator(), j.getJoinRelName());
				convertedJoins.add(newJ);
			}
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
	
	protected ITableDataFrame getSourceFrame() {
		GenRowStruct frameGrs = this.store.getNoun(this.keysToGet[0]);
		if(frameGrs != null && !frameGrs.isEmpty()) {
			return (ITableDataFrame) frameGrs.get(0);
		}
		
		List<NounMetadata> frameCur = this.curRow.getNounsOfType(PixelDataType.FRAME);
		if(frameCur != null && !frameCur.isEmpty()) {
			return (ITableDataFrame) frameCur.get(0).getValue();
		}
		
		throw new IllegalArgumentException("Must define the source frame");
	}
	
	protected ITableDataFrame getTargetFrame() {
		GenRowStruct frameGrs = this.store.getNoun(this.keysToGet[1]);
		if(frameGrs != null && !frameGrs.isEmpty()) {
			return (ITableDataFrame) frameGrs.get(0);
		}
		
		List<NounMetadata> frameCur = this.curRow.getNounsOfType(PixelDataType.FRAME);
		if(frameCur != null && frameCur.size() == 2) {
			return (ITableDataFrame) frameCur.get(1).getValue();
		}
		
		throw new IllegalArgumentException("Must define the target frame");
	}
	
	protected List<String> getSourceColumns() {
		List<String> columns = new Vector<String>();

		GenRowStruct sourceColGrs = this.store.getNoun(this.keysToGet[2]);
		if (sourceColGrs != null && !sourceColGrs.isEmpty()) {
			for (int selectIndex = 0; selectIndex < sourceColGrs.size(); selectIndex++) {
				String column = sourceColGrs.get(selectIndex) + "";
				columns.add(column);
			}
			return columns;
		}
		throw new IllegalArgumentException("Must define the source columns");
	}
	
	protected List<String> getTargetColumns() {
		List<String> columns = new Vector<String>();

		GenRowStruct colGrs = this.store.getNoun(this.keysToGet[3]);
		if (colGrs != null && !colGrs.isEmpty()) {
			for (int selectIndex = 0; selectIndex < colGrs.size(); selectIndex++) {
				String column = colGrs.get(selectIndex) + "";
				columns.add(column);
			}
			return columns;
		}
		throw new IllegalArgumentException("Must define the target columns");

	}
	
	protected List<String> getJoinTypes() {
		List<String> joins = new Vector<String>();

		GenRowStruct joinGrs = this.store.getNoun(this.keysToGet[4]);
		if (joinGrs != null && !joinGrs.isEmpty()) {
			for (int selectIndex = 0; selectIndex < joinGrs.size(); selectIndex++) {
				String join = joinGrs.get(selectIndex) + "";
				joins.add(join);
			}
			return joins;
		}
		throw new IllegalArgumentException("Must define the join types");

	}
	
//	protected List<Join> getJoins() {
//		List<Join> joins = new Vector<Join>();
//		// try specific key
//		{
//			GenRowStruct grs = this.store.getNoun(this.keysToGet[2]);
//			if(grs != null && !grs.isEmpty()) {
//				joins = grs.getAllJoins();
//				if(joins != null && !joins.isEmpty()) {
//					return joins;
//				}
//			}
//		}
//		
//		List<NounMetadata> joinsCur = this.curRow.getNounsOfType(PixelDataType.JOIN);
//		if(joinsCur != null && !joinsCur.isEmpty()) {
//			int size = joinsCur.size();
//			for(int i = 0; i < size; i++) {
//				joins.add( (Join) joinsCur.get(i).getValue());
//			}
//			
//			return joins;
//		}
//		
//		throw new IllegalArgumentException("Could not find the columns for the join");
//	}
	
	
	protected List<Join> getJoins() {
		List<String> sourceCols =  getSourceColumns();
		
		List<String> targetCols =  getTargetColumns();
		List<String> joinTypes =  getJoinTypes();
		
		if(sourceCols.size() != targetCols.size()) {
			throw new IllegalArgumentException("Uneven number of source columns to target columns");
		}
		
		if(joinTypes.size()!= 1 && joinTypes.size() != sourceCols.size()) {
			throw new IllegalArgumentException("Uneven number of joins to columns");
		}
		//if only 1 join type is given, fill out the list
		if(joinTypes.size() == 1 && sourceCols.size() > 1) {
			while(joinTypes.size()!=sourceCols.size()) {
				joinTypes.add(joinTypes.get(0));
			}
		}
		
		List<Join> joins = new Vector<Join>();

		for(int i = 0; i<sourceCols.size(); i++) {
			Join j = new Join(targetCols.get(i), joinTypes.get(i),sourceCols.get(i));
			joins.add(j);
		}
		
		return joins;
	}

	public String getName()
	{
		return "MergeFrames";
	}

}