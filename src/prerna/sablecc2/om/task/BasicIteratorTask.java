package prerna.sablecc2.om.task;

import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Set;

import prerna.algorithm.api.ITableDataFrame;
import prerna.ds.OwlTemporalEngineMeta;
import prerna.ds.h2.H2Frame;
import prerna.engine.api.IEngineWrapper;
import prerna.engine.api.IHeadersDataRow;
import prerna.engine.api.IRawSelectWrapper;
import prerna.query.querystruct.HardQueryStruct;
import prerna.query.querystruct.QueryStruct2;
import prerna.query.querystruct.selectors.IQuerySelector;
import prerna.query.querystruct.selectors.QueryColumnOrderBySelector;
import prerna.query.querystruct.transform.QSAliasToPhysicalConverter;
import prerna.rdf.engine.wrappers.WrapperManager;
import prerna.util.Utility;

public class BasicIteratorTask extends AbstractTask {

	private QueryStruct2 qs;
	private long startLimit = -1;
	private long startOffset = -1;
	private transient Iterator<IHeadersDataRow> iterator;
	
	public BasicIteratorTask(QueryStruct2 qs) {
		this.qs = qs;
		// this is important so we dont override
		// the existing limit of the query
		// within the query optimization
		this.startLimit = this.qs.getLimit();
		this.startOffset = this.qs.getOffset();
	}
	
	public BasicIteratorTask(Iterator<IHeadersDataRow> iterator) {
		this.iterator = iterator;
	}
	
	public BasicIteratorTask(QueryStruct2 qs, Iterator<IHeadersDataRow> iterator) {
		this(qs);
		this.iterator = iterator;
	}
	
	@Override
	public boolean hasNext() {
		if(this.iterator == null && this.qs == null) {
			return false;
		} else if( this.qs != null && this.iterator == null) {
			generateIterator(this.qs);
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
			generateIterator(this.qs);
		}
	}
	
	private void generateIterator(QueryStruct2 qs) {
		if(qs.getQsType() == QueryStruct2.QUERY_STRUCT_TYPE.ENGINE || 
				qs.getQsType() == QueryStruct2.QUERY_STRUCT_TYPE.RAW_ENGINE_QUERY) {
			iterator = WrapperManager.getInstance().getRawWrapper(qs.retrieveQueryStructEngine(), qs);
		} else {
			ITableDataFrame frame = qs.getFrame();
			frame.setLogger(this.logger);
			optimizeFrame(frame, qs.getOrderBy());
			iterator = frame.query(qs);
		}
		setHeaderInfo(qs.getHeaderInfo());
		setSortInfo(qs.getSortInfo());
		setFilterInfo(qs.getExplicitFilters());
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
				if(!QueryStruct2.PRIM_KEY_PLACEHOLDER.equals(col) && !indexedCols.contains(col)) {
					hFrame.addColumnIndex(col);
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
		if(this.qs != null && !(this.qs instanceof HardQueryStruct) ) {
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
				this.qs.addOrderBy(firstSelector.getAlias(), null, "ASC");
				addedOrder = true;
			}
			generateIterator(this.qs);
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
	
	public QueryStruct2 getQueryStruct() {
		return this.qs;
	}
	
	//TODO: come back to this and why it is used
	@Deprecated
	public Iterator getIterator() {
		return this.iterator;
	}
	
}
