package prerna.sablecc2.om.task;

import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Set;

import prerna.algorithm.api.ITableDataFrame;
import prerna.ds.OwlTemporalEngineMeta;
import prerna.ds.h2.H2Frame;
import prerna.ds.r.RDataTable;
import prerna.engine.api.IEngineWrapper;
import prerna.engine.api.IHeadersDataRow;
import prerna.engine.api.IRawSelectWrapper;
import prerna.query.querystruct.HardSelectQueryStruct;
import prerna.query.querystruct.SelectQueryStruct;
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
			String[] types = null;
			if(this.iterator instanceof IRawSelectWrapper) {
				types = ((IRawSelectWrapper) this.iterator).getTypes();
			}
			if(types != null) {
				for(int i = 0; i < headerInfo.size(); i++) {
					headerInfo.get(i).put("type", Utility.getCleanDataType(types[i]));
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
		if(qs.getQsType() == SelectQueryStruct.QUERY_STRUCT_TYPE.ENGINE || 
				qs.getQsType() == SelectQueryStruct.QUERY_STRUCT_TYPE.RAW_ENGINE_QUERY) {
			iterator = WrapperManager.getInstance().getRawWrapper(qs.retrieveQueryStructEngine(), qs);
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
			Set<String> indexedCols = hFrame.getColumnsWithIndexes();
			OwlTemporalEngineMeta meta = hFrame.getMetaData();
			for(int i = 0; i < orderBys.size(); i++) {
				QueryColumnOrderBySelector origOrderS = orderBys.get(i);
				QueryColumnOrderBySelector convertedOrderByS = QSAliasToPhysicalConverter.convertOrderBySelector(origOrderS, meta);
				String col = convertedOrderByS.getColumn();
				if(!SelectQueryStruct.PRIM_KEY_PLACEHOLDER.equals(col) && !indexedCols.contains(col)) {
					hFrame.addColumnIndex(col);
				}
			}
		} else if(dataframe instanceof RDataTable) {
			//TODO: come back to this for testing
//			RDataTable rFrame = (RDataTable) dataframe;
//			OwlTemporalEngineMeta meta = rFrame.getMetaData();
//			for(int i = 0; i < orderBys.size(); i++) {
//				QueryColumnOrderBySelector origOrderS = orderBys.get(i);
//				QueryColumnOrderBySelector convertedOrderByS = QSAliasToPhysicalConverter.convertOrderBySelector(origOrderS, meta);
//				String col = convertedOrderByS.getColumn();
//				if(!SelectQueryStruct.PRIM_KEY_PLACEHOLDER.equals(col)) {
//					rFrame.addColumnIndex(col);
//				}
//			}
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
		setHeaderInfo(qs.getHeaderInfo());
		setSortInfo(qs.getSortInfo());
		setFilterInfo(qs.getExplicitFilters());
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
