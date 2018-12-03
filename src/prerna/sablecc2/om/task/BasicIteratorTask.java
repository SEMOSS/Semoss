package prerna.sablecc2.om.task;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Set;
import java.util.Vector;
import java.util.stream.Collectors;

import prerna.algorithm.api.ITableDataFrame;
import prerna.algorithm.api.SemossDataType;
import prerna.ds.OwlTemporalEngineMeta;
import prerna.ds.h2.H2Frame;
import prerna.ds.r.RDataTable;
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
import prerna.query.querystruct.selectors.QueryColumnOrderBySelector;
import prerna.query.querystruct.transform.QSAliasToPhysicalConverter;
import prerna.rdf.engine.wrappers.WrapperManager;
import prerna.util.Utility;

public class BasicIteratorTask extends AbstractTask {

	private SelectQueryStruct qs;
	private long startLimit = -1;
	private long startOffset = -1;
	private transient Iterator<IHeadersDataRow> iterator;
	private boolean grabFromWrapper = false;
	
	public BasicIteratorTask(SelectQueryStruct qs) {
		this.qs = qs;
		// this is important so we dont override
		// the existing limit of the query
		// within the query optimization
		this.startLimit = this.qs.getLimit();
		this.startOffset = this.qs.getOffset();
		setQsMetadata(this.qs);
	}
	
	public BasicIteratorTask(Iterator<IHeadersDataRow> iterator) {
		this.iterator = iterator;
	}
	
	public BasicIteratorTask(SelectQueryStruct qs, Iterator<IHeadersDataRow> iterator) {
		this(qs);
		this.iterator = iterator;
	}
	
	@Override
	public boolean hasNext() {
		if(this.iterator == null && this.qs == null) {
			return false;
		} else if( this.qs != null && this.iterator == null) {
			generateIterator(this.qs, false);
		}
		
		if(this.iterator == null) {
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
		return iterator.next();
	}
	
	@Override
	public void setHeaderInfo(List<Map<String, Object>> headerInfo) {
		this.headerInfo = headerInfo;
		//TODO: bad :(
		//need to create a proper iterate object that will get this info
		//instead of how it is set up which takes it from the QS
		if(this.headerInfo != null) {
			if(this.iterator instanceof IRawSelectWrapper) {
				SemossDataType[] sTypes = ((IRawSelectWrapper) this.iterator).getTypes();
				String[] types = Arrays.asList(sTypes).stream()
						.map(p -> p == null ? "STRING" : p)
						.map(p -> p.toString())
						.map(p -> (p.equals("DOUBLE") || p.equals("INT") ? "NUMBER" : p))
						.collect(Collectors.toList()).toArray(new String[]{});
				for(int i = 0; i < headerInfo.size(); i++) {
					headerInfo.get(i).put("type", Utility.getCleanDataType(types[i]));
				}
			}
		} else if(this.grabFromWrapper && (this.headerInfo == null || this.headerInfo.isEmpty()) ) {
			if(this.iterator == null) {
				generateIterator(this.qs, false);
			}
			if(this.iterator instanceof IRawSelectWrapper) {
				String[] headers = ((IRawSelectWrapper) this.iterator).getHeaders();
				SemossDataType[] sTypes = ((IRawSelectWrapper) this.iterator).getTypes();
				String[] types = Arrays.asList(sTypes).stream()
						.map(p -> p == null ? "STRING" : p)
						.map(p -> p.toString())
						.map(p -> (p.equals("DOUBLE") || p.equals("INT") ? "NUMBER" : p))
						.collect(Collectors.toList()).toArray(new String[]{});
				this.headerInfo = new Vector<Map<String, Object>>();
				for(int i = 0 ; i < headers.length; i++) {
					Map<String, Object> headerMap = new HashMap<String, Object>();
					headerMap.put("alias", headers[i]);
					headerMap.put("derived", false);
					headerMap.put("header", headers[i]);
					headerMap.put("type", Utility.getCleanDataType(types[i]));
					this.headerInfo.add(headerMap);
				}
			}
		}
	}
	
	@Override
	public void cleanUp() {
		if(this.iterator instanceof IEngineWrapper) {
			IEngineWrapper x = ((IEngineWrapper) this.iterator);
			x.cleanUp();
		}

		// help java gc
		if(this.taskOptions != null) {
			this.taskOptions.clear();
			this.taskOptions = null;
		}
		if(this.headerInfo != null) {
			this.headerInfo.clear();
			this.headerInfo = null;
		}
		if(this.sortInfo != null) {
			this.sortInfo.clear();
			this.sortInfo = null;
		}
		if(this.filterInfo != null) {
			this.filterInfo.clear();
			this.filterInfo = null;
		}
	}
	
	@Override
	public void reset() {
		cleanUp();
		if(this.qs != null) {
			this.qs.setLimit(this.startLimit);
			this.qs.setOffSet(this.startOffset);
			this.internalOffset = 0;
			generateIterator(this.qs, true);
		}
	}
	
	private void generateIterator(SelectQueryStruct qs, boolean overrideImplicitFilters) {
		SelectQueryStruct.QUERY_STRUCT_TYPE qsType = qs.getQsType();
		if(qsType == SelectQueryStruct.QUERY_STRUCT_TYPE.ENGINE || qsType == SelectQueryStruct.QUERY_STRUCT_TYPE.RAW_ENGINE_QUERY) {
			iterator = WrapperManager.getInstance().getRawWrapper(qs.retrieveQueryStructEngine(), qs);
		} else if(qsType == SelectQueryStruct.QUERY_STRUCT_TYPE.CSV_FILE) {
			iterator = new CsvFileIterator((CsvQueryStruct) qs);
		} else if(qsType == SelectQueryStruct.QUERY_STRUCT_TYPE.EXCEL_FILE) {
			iterator = ExcelWorkbookFileHelper.buildSheetIterator((ExcelQueryStruct) qs); //new ExcelFileIterator((ExcelQueryStruct) qs);
		} else {
			ITableDataFrame frame = qs.getFrame();
			if(overrideImplicitFilters) {
				qs.setImplicitFilters(frame.getFrameFilters());
			}
			frame.setLogger(this.logger);
			optimizeFrame(frame, qs.getOrderBy());
			iterator = frame.query(qs);
		}
		setQsMetadata(qs);
	}
	
	private void optimizeFrame(ITableDataFrame dataframe, List<QueryColumnOrderBySelector> orderBys) {
		if (dataframe instanceof H2Frame) {
			H2Frame hFrame = (H2Frame) dataframe;
			Set<String> hIndexedCols = hFrame.getColumnsWithIndexes();
			OwlTemporalEngineMeta meta = hFrame.getMetaData();
			for(int i = 0; i < orderBys.size(); i++) {
				QueryColumnOrderBySelector origOrderS = orderBys.get(i);
				QueryColumnOrderBySelector convertedOrderByS = QSAliasToPhysicalConverter.convertOrderBySelector(origOrderS, meta);
				String col = convertedOrderByS.getColumn();
				if(!SelectQueryStruct.PRIM_KEY_PLACEHOLDER.equals(col) && !hIndexedCols.contains(col)) {
					hFrame.addColumnIndex(col);
				}
			}
		} else if(dataframe instanceof RDataTable) {
			RDataTable rFrame = (RDataTable) dataframe;
			OwlTemporalEngineMeta meta = rFrame.getMetaData();
			Set<String> rIndexedCols = rFrame.getColumnsWithIndexes();
			for(int i = 0; i < orderBys.size(); i++) {
				QueryColumnOrderBySelector origOrderS = orderBys.get(i);
				QueryColumnOrderBySelector convertedOrderByS = QSAliasToPhysicalConverter.convertOrderBySelector(origOrderS, meta);
				String col = convertedOrderByS.getColumn();
				if(!SelectQueryStruct.PRIM_KEY_PLACEHOLDER.equals(col) && !rIndexedCols.contains(col)) {
					rFrame.addColumnIndex(col);
				}
			}
		}
	}
	
	public void optimizeQuery(int collectNum) {
		// already have a limit defined
		// just continue;
		if(this.startLimit > 0) {
			return;
		}
		if(this.qs != null && !(this.qs instanceof HardSelectQueryStruct) ) {
			if(collectNum < 0) {
				// from this point on
				// we will just collect everything
				this.qs.setLimit(-1);
			} else {
				this.qs.setLimit(collectNum);
			}
			long offset = 0;
			if(this.startOffset > 0) {
				offset = this.startOffset;
			}
			this.qs.setOffSet(offset + this.internalOffset);
			boolean addedOrder = false;
			if(this.qs.getOrderBy().isEmpty()) {
				// need to add an implicit order
				IQuerySelector firstSelector = this.qs.getSelectors().get(0);
				if(firstSelector.getSelectorType() == SELECTOR_TYPE.COLUMN) {
					this.qs.addOrderBy(firstSelector.getQueryStructName(), "ASC");
				} else {
					this.qs.addOrderBy(firstSelector.getAlias(), null, "ASC");
				}
				addedOrder = true;
			}
			generateIterator(this.qs, false);
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
	
	private void setQsMetadata(SelectQueryStruct qs) {
		if(qs.getQsType() == QUERY_STRUCT_TYPE.RAW_ENGINE_QUERY || 
				qs.getQsType() == QUERY_STRUCT_TYPE.RAW_FRAME_QUERY) {
			this.grabFromWrapper = true;
			setSortInfo(new Vector<Map<String, Object>>());
			setFilterInfo(new GenRowFilters());
		} else {
			setHeaderInfo(qs.getHeaderInfo());
			setSortInfo(qs.getSortInfo());
			setFilterInfo(qs.getExplicitFilters());
		}
	}
	
	@Override
	public List<Map<String, Object>> getHeaderInfo() {
		if(this.grabFromWrapper && (this.headerInfo == null || this.headerInfo.isEmpty()) ) {
			setHeaderInfo(null);
		}
		return this.headerInfo;
	}
	
	public SelectQueryStruct getQueryStruct() {
		return this.qs;
	}
	
	//TODO: come back to this and why it is used
	@Deprecated
	public Iterator getIterator() {
		return this.iterator;
	}
	
}
