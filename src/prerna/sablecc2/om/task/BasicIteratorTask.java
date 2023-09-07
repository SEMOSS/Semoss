package prerna.sablecc2.om.task;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Set;
import java.util.stream.Collectors;

import prerna.algorithm.api.ITableDataFrame;
import prerna.algorithm.api.SemossDataType;
import prerna.ds.OwlTemporalEngineMeta;
import prerna.ds.r.RDataTable;
import prerna.ds.rdbms.AbstractRdbmsFrame;
import prerna.ds.shared.CachedIterator;
import prerna.ds.shared.RawCachedWrapper;
import prerna.ds.util.flatfile.CsvFileIterator;
import prerna.engine.api.IEngineWrapper;
import prerna.engine.api.IHeadersDataRow;
import prerna.engine.api.IRawSelectWrapper;
import prerna.poi.main.helper.excel.ExcelWorkbookFileHelper;
import prerna.query.querystruct.AbstractQueryStruct.QUERY_STRUCT_TYPE;
import prerna.query.querystruct.CsvQueryStruct;
import prerna.query.querystruct.ExcelQueryStruct;
import prerna.query.querystruct.HardSelectQueryStruct;
import prerna.query.querystruct.SelectQueryStruct;
import prerna.query.querystruct.filters.GenRowFilters;
import prerna.query.querystruct.selectors.IQuerySelector;
import prerna.query.querystruct.selectors.IQuerySelector.SELECTOR_TYPE;
import prerna.query.querystruct.selectors.IQuerySort;
import prerna.query.querystruct.selectors.QueryColumnOrderBySelector;
import prerna.query.querystruct.selectors.QueryColumnSelector;
import prerna.query.querystruct.selectors.QueryCustomOrderBy;
import prerna.query.querystruct.transform.QSAliasToPhysicalConverter;
import prerna.rdf.engine.wrappers.WrapperManager;
import prerna.sablecc2.om.execptions.SemossPixelException;
import prerna.util.Constants;
import prerna.util.Utility;

public class BasicIteratorTask extends AbstractTask {

	private SelectQueryStruct qs;
	private long startLimit = -1;
	private long startOffset = -1;
	private transient IRawSelectWrapper iterator;
	private boolean grabFromWrapper = false;
	private boolean grabTypesFromWrapper = false;
	
	private long collectLimit = -1;
	private long collectCounter = 0;
	
	
	public BasicIteratorTask(SelectQueryStruct qs) {
		this.qs = qs;
		// this is important so we dont override
		// the existing limit of the query
		// within the query optimization
		this.startLimit = this.qs.getLimit();
		this.startOffset = this.qs.getOffset();
		setQsMetadata(this.qs);
	}
	
	public BasicIteratorTask(IRawSelectWrapper iterator) {
		this.iterator = iterator;
	}
	
	public BasicIteratorTask(SelectQueryStruct qs, IRawSelectWrapper iterator) {
		this(qs);
		this.iterator = iterator;
	}
	
	/**
	 * Collect data from an iterator
	 * Or return defined outputData
	 * @throws Exception 
	 */
	@Override
	public Map<String, Object> collect(boolean meta) throws Exception {
		Map<String, Object> collectedData = new HashMap<String, Object>(10);
		collectedData.put("data", getData());
		if(meta) {
			collectedData.put("headerInfo", this.getHeaderInfo());
			if(this.taskOptions != null && !this.taskOptions.isEmpty()) {
				collectedData.put("format", getFormatMap());
				collectedData.put("taskOptions", this.taskOptions.getOptions());
				collectedData.put("sortInfo", this.sortInfo);
				collectedData.put("filterInfo", this.filterInfo);
				if(this.qs == null || !this.qs.getBigDataEngine()) {
					long numRows = TaskUtility.getNumRows(this);
					if(numRows > 0) {
						collectedData.put("numRows", numRows);
					}
				}
			}
		}
		collectedData.put("sources", getSource());
		collectedData.put("taskId", this.id);
		collectedData.put("numCollected", this.numCollect);
		return collectedData;
	}
	
	@Override
	public boolean hasNext() {
		if(this.iterator == null && this.qs == null) {
			return false;
		} else if( this.qs != null && this.iterator == null) {
			try {
				generateIterator();
			} catch (Exception e) {
				e.printStackTrace();
				throw new SemossPixelException(e.getMessage());
			}
		}
		
		if(this.iterator == null) {
			return false;
		}
		
		// are we adding a collect limit on the task?
		if(collectLimit > -1 && collectCounter >= collectLimit) {
			return false;
		}
		
		return iterator.hasNext();
	}
	
	@Override
	public IHeadersDataRow next() {
		if(this.iterator == null) {
			throw new NoSuchElementException("Could not find additional elements for iterator");
		}
		this.internalOffset++;
		if(this.collectLimit > -1) {
			collectCounter++;
		}
		return iterator.next();
	}
	
	@Override
	public void setHeaderInfo(List<Map<String, Object>> headerInfo) {
		this.headerInfo = headerInfo;
		//TODO: bad :(
		//need to create a proper iterate object that will get this info
		//instead of how it is set up which takes it from the QS
		if(this.headerInfo != null && this.grabTypesFromWrapper) {
			if(this.iterator == null) {
				try {
					generateIterator();
				} catch (Exception e) {
					e.printStackTrace();
					throw new SemossPixelException(e.getMessage());
				}
			}
			if(this.iterator instanceof IRawSelectWrapper) {
				SemossDataType[] sTypes = ((IRawSelectWrapper) this.iterator).getTypes();
				String[] types = Arrays.asList(sTypes).stream()
						.map(p -> p == null ? "STRING" : p)
						.map(p -> p.toString())
						.map(p -> (p.equals("DOUBLE") || p.equals("INT") ? "NUMBER" : p))
						.collect(Collectors.toList()).toArray(new String[sTypes.length]);
				// this needs to be adjusted for the max columns
				for(int i = 0; i < headerInfo.size(); i++) {
					// TODO: will eventually stop sending type and doing the above processing
					headerInfo.get(i).put("type", Utility.getCleanDataType(types[i]));
					headerInfo.get(i).put("dataType", sTypes[i].toString());
				}
			}
		} else if(this.grabFromWrapper && (this.headerInfo == null || this.headerInfo.isEmpty()) ) {
			if(this.iterator == null) {
				try {
					generateIterator();
				} catch (Exception e) {
					e.printStackTrace();
					throw new SemossPixelException(e.getMessage());
				}
			}
			if(this.iterator instanceof IRawSelectWrapper) {
				String[] headers = ((IRawSelectWrapper) this.iterator).getHeaders();
				SemossDataType[] sTypes = ((IRawSelectWrapper) this.iterator).getTypes();
				String[] types = Arrays.asList(sTypes).stream()
						.map(p -> p == null ? "STRING" : p)
						.map(p -> p.toString())
						.map(p -> (p.equals("DOUBLE") || p.equals("INT") ? "NUMBER" : p))
						.collect(Collectors.toList()).toArray(new String[sTypes.length]);
				this.headerInfo = new ArrayList<Map<String, Object>>();
				for(int i = 0 ; i < headers.length; i++) {
					Map<String, Object> headerMap = new HashMap<String, Object>();
					headerMap.put("alias", headers[i]);
					headerMap.put("derived", false);
					headerMap.put("header", headers[i]);
					// TODO: will eventually stop sending type and doing the above processing
					headerMap.put("type", Utility.getCleanDataType(types[i]));
					if(sTypes[i] != null)
						headerMap.put("dataType", sTypes[i].toString());
					else
						headerMap.put("dataType", SemossDataType.STRING.toString());
						
					this.headerInfo.add(headerMap);
				}
			}
		}
	}
	
	@Override
	public void close() throws IOException {
		if(this.iterator instanceof IEngineWrapper) {
			IEngineWrapper x = ((IEngineWrapper) this.iterator);
			x.close();
		}

//		// help java gc
//		this.taskOptions = null;
//		if(this.headerInfo != null) {
//			this.headerInfo.clear();
//			this.headerInfo = null;
//		}
//		if(this.sortInfo != null) {
//			this.sortInfo.clear();
//			this.sortInfo = null;
//		}
//		if(this.filterInfo != null) {
//			this.filterInfo.clear();
//			this.filterInfo = null;
//		}
	}
	
	@Override
	public void reset() throws Exception {
		try {
			close();
		} catch(IOException e) {
			classLogger.error(Constants.STACKTRACE, e);
		}
		if(this.qs != null) {
			this.qs.setLimit(this.startLimit);
			this.qs.setOffSet(this.startOffset);
			this.internalOffset = 0;
			generateIterator();
		}
	}
	
	private void generateIterator() throws Exception {
		// I need a way here to see if this is already done as a iterator and if so take a copy of it
		SelectQueryStruct.QUERY_STRUCT_TYPE qsType = this.qs.getQsType();
		if(qsType == SelectQueryStruct.QUERY_STRUCT_TYPE.ENGINE || qsType == SelectQueryStruct.QUERY_STRUCT_TYPE.RAW_ENGINE_QUERY
				|| qsType == SelectQueryStruct.QUERY_STRUCT_TYPE.RAW_JDBC_ENGINE_QUERY) {
			iterator = WrapperManager.getInstance().getRawWrapper(this.qs.retrieveQueryStructEngine(), this.qs);
		} else if(qsType == SelectQueryStruct.QUERY_STRUCT_TYPE.CSV_FILE) {
			iterator = new CsvFileIterator((CsvQueryStruct) this.qs);
		} else if(qsType == SelectQueryStruct.QUERY_STRUCT_TYPE.EXCEL_FILE) {
			iterator = ExcelWorkbookFileHelper.buildSheetIterator((ExcelQueryStruct) this.qs); //new ExcelFileIterator((ExcelQueryStruct) qs);
		} else {
			ITableDataFrame frame = this.qs.getFrame();
			frame.setLogger(this.classLogger);
			optimizeFrame(frame, this.qs.getCombinedOrderBy());
			boolean taskOptionsExists; 
			if (this.taskOptions != null && !(this.taskOptions.isEmpty())){
				taskOptionsExists = true;
			} else {
				taskOptionsExists = false; 
			}
			if(this.qs.getPragmap() == null) {	
				Map prags=  new java.util.HashMap();
				prags.put(Constants.TASK_OPTIONS_EXIST, taskOptionsExists);
				this.qs.setPragmap(prags);
			} else{
				Map prags = this.qs.getPragmap();
				prags.put(Constants.TASK_OPTIONS_EXIST, taskOptionsExists);
				this.qs.setPragmap(prags);
			}
			if(this.classLogger != null) {
				frame.setLogger(this.classLogger);
			}
			iterator = frame.query(this.qs);
		}
		setQsMetadata(this.qs);
	}
	
	private void optimizeFrame(ITableDataFrame dataframe, List<IQuerySort> orderBys) {
		if (dataframe instanceof AbstractRdbmsFrame) {
			AbstractRdbmsFrame rdbmsFrame = (AbstractRdbmsFrame) dataframe;
			OwlTemporalEngineMeta meta = rdbmsFrame.getMetaData();
			for(int i = 0; i < orderBys.size(); i++) {
				IQuerySort origOrderS = orderBys.get(i);
				IQuerySort convertedOrderByS = QSAliasToPhysicalConverter.convertOrderByOperation(origOrderS, meta);
				if(convertedOrderByS.getQuerySortType() == IQuerySort.QUERY_SORT_TYPE.COLUMN) {
					QueryColumnOrderBySelector columnOrderBy = (QueryColumnOrderBySelector) convertedOrderByS;
					String table = columnOrderBy.getTable();
					String col = columnOrderBy.getColumn();
					if(!SelectQueryStruct.PRIM_KEY_PLACEHOLDER.equals(col)) {
						rdbmsFrame.getBuilder().addColumnIndex(table, col);
					}
				} else if(convertedOrderByS.getQuerySortType() == IQuerySort.QUERY_SORT_TYPE.CUSTOM) {
					QueryCustomOrderBy customOrderBy = (QueryCustomOrderBy) convertedOrderByS;
					QueryColumnSelector columnToSort = customOrderBy.getColumnToSort();
					String table = columnToSort.getTable();
					String col = columnToSort.getColumn();
					if(!SelectQueryStruct.PRIM_KEY_PLACEHOLDER.equals(col)) {
						rdbmsFrame.getBuilder().addColumnIndex(table, col);
					}
				}
			}
		} else if(dataframe instanceof RDataTable) {
			RDataTable rFrame = (RDataTable) dataframe;
			OwlTemporalEngineMeta meta = rFrame.getMetaData();
			Set<String> rIndexedCols = rFrame.getColumnsWithIndexes();
			for(int i = 0; i < orderBys.size(); i++) {
				IQuerySort origOrderS = orderBys.get(i);
				IQuerySort convertedOrderByS = QSAliasToPhysicalConverter.convertOrderByOperation(origOrderS, meta);
				if(convertedOrderByS.getQuerySortType() == IQuerySort.QUERY_SORT_TYPE.COLUMN) {
					QueryColumnOrderBySelector columnOrderBy = (QueryColumnOrderBySelector) convertedOrderByS;
					String col = columnOrderBy.getColumn();
					if(!SelectQueryStruct.PRIM_KEY_PLACEHOLDER.equals(col) && !rIndexedCols.contains(col)) {
						rFrame.addColumnIndex(col);
					}
				} else if(convertedOrderByS.getQuerySortType() == IQuerySort.QUERY_SORT_TYPE.CUSTOM) {
					QueryCustomOrderBy customOrderBy = (QueryCustomOrderBy) convertedOrderByS;
					QueryColumnSelector columnToSort = customOrderBy.getColumnToSort();
					String col = columnToSort.getColumn();
					if(!SelectQueryStruct.PRIM_KEY_PLACEHOLDER.equals(col) && !rIndexedCols.contains(col)) {
						rFrame.addColumnIndex(col);
					}
				}
			}
		}
	}
	
	public void optimizeQuery(int collectNum) throws Exception {
		if(this.isOptimize) {
			if(this.qs != null && !(this.qs instanceof HardSelectQueryStruct) ) {
				this.classLogger.info(SelectQueryStruct.getExecutingQueryMessage(this.qs));

				// if no limit defined
				if(this.startLimit < 0) {
					if(collectNum < 0) {
						// from this point on
						// we will just collect everything
						this.qs.setLimit(-1);
					} else {
						this.qs.setLimit(collectNum);
					}
				}
				
				long offset = 0;
				if(this.startOffset > 0) {
					offset = this.startOffset;
				}
				this.qs.setOffSet(offset + this.internalOffset);
				boolean addedOrder = false;
				boolean setImplicitOrderBy = true;
				Object value = this.qs.getPragmap().get(Constants.IMPLICIT_ORDER);
				if (value != null) {
					 setImplicitOrderBy = Boolean.parseBoolean(value.toString());
				}
				
				if(this.qs.getCombinedOrderBy().isEmpty() && !this.qs.getBigDataEngine() && setImplicitOrderBy) {
					// need to add an implicit order
					IQuerySelector firstSelector = this.qs.getSelectors().get(0);
					if(firstSelector.getSelectorType() == SELECTOR_TYPE.COLUMN) {
						this.qs.addOrderBy(firstSelector.getQueryStructName(), "ASC");
					} else {
						this.qs.addOrderBy(firstSelector.getAlias(), null, "ASC");
					}
					addedOrder = true;
				}
				generateIterator();
				this.classLogger.info("Finished executing the query");
				// we got the iterator
				// if we added an order, remove it
				if(addedOrder) {
					this.qs.getOrderBy().clear();
					// also clear it on the task
					// we dont want to send it to the FE
					setSortInfo(qs.getSortInfo());
				}
			}
		}
	}
	
	private void setQsMetadata(SelectQueryStruct qs) {
		if(qs.getQsType() == QUERY_STRUCT_TYPE.RAW_ENGINE_QUERY || 
				qs.getQsType() == QUERY_STRUCT_TYPE.RAW_FRAME_QUERY) {
			this.grabFromWrapper = true;
			setSortInfo(new ArrayList<Map<String, Object>>());
			setFilterInfo(new GenRowFilters());
		} else {
			setHeaderInfo(qs.getHeaderInfo());
			setSortInfo(qs.getSortInfo());
			setFilterInfo(qs.getExplicitFilters());
			// set this at the end because i dont want to 
			// get the iterator in the constructor
			// of this class
			this.grabTypesFromWrapper = true;
		}
	}
	
	@Override
	public List<Map<String, Object>> getHeaderInfo() {
		if(this.grabFromWrapper && (this.headerInfo == null || this.headerInfo.isEmpty()) ) {
			setHeaderInfo(null);
		} else if(this.grabTypesFromWrapper) {
			setHeaderInfo(this.headerInfo);
			this.grabTypesFromWrapper = false;
		}
		return this.headerInfo;
	}
	
	@Override
	public List<Map<String, String>> getSource() {
		List<Map<String, String>> sources = new ArrayList<Map<String, String>>();
		if(this.qs != null) {
			sources.add(qs.getSourceMap());
		}
		return sources;
	}
	
	@Override
	public RawCachedWrapper createCache() throws Exception {
		// since we lazy execute the iterator
		// make sure it exists
		if(this.qs != null && this.iterator == null) {
			generateIterator();
		}
		// creates a new cache to be used
		ITableDataFrame frame = this.qs.getFrame();
		RawCachedWrapper retWrapper = null;
		// will copy the iterator
		if(iterator instanceof RawCachedWrapper) {
			retWrapper = (RawCachedWrapper) iterator;
		} else {
			CachedIterator it = new CachedIterator();
			it.setHeaders(iterator.getHeaders());
			it.setColTypes(iterator.getTypes());
			it.setQuery(iterator.getQuery());
			it.setFrame(frame);
			RawCachedWrapper wrapper = new RawCachedWrapper();
			wrapper.setIterator(it);
			retWrapper = wrapper;
		}
		return retWrapper;
	}
	
	// gets a specific pragma value
	@Override
	public String getPragma(String pragma) {
		String retString = null;
		if(qs.getPragmap() != null && qs.getPragmap().containsKey(pragma)) {
			retString = (String)qs.getPragmap().get(pragma);
		}
		return retString;
	}
	
	public SelectQueryStruct getQueryStruct() {
		return this.qs;
	}
	
	public void setCollectLimit(long collectLimit) {
		this.collectLimit = collectLimit;
	}
	
	public IRawSelectWrapper getIterator() throws Exception {
		if(this.qs != null && this.iterator == null) {
			generateIterator();
		}
		return this.iterator;
	}
	
}
