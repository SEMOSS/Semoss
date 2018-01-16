package prerna.ds;

import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.tinkergraph.structure.TinkerVertex;

import prerna.engine.api.IHeadersDataRow;
import prerna.om.HeadersDataRow;
import prerna.query.querystruct.QueryStruct2;
import prerna.query.querystruct.selectors.IQuerySelector;
import prerna.query.querystruct.selectors.QueryFunctionSelector;

public class TinkerHeadersDataRowIterator2 implements Iterator<IHeadersDataRow> {

	private QueryStruct2 qs;
	private Iterator baseIterator;
	private String[] headerAlias;
	private String[] header;
	private String[] headerOrdering;

	public TinkerHeadersDataRowIterator2(Iterator composeIterator, QueryStruct2 qs) {
		this.baseIterator = composeIterator;
		this.qs = qs;
		flushOutHeaders(this.qs.getSelectors(), null);
	}
	
	public TinkerHeadersDataRowIterator2(Iterator composeIterator, QueryStruct2 qs, OwlTemporalEngineMeta meta) {
		this.baseIterator = composeIterator;
		this.qs = qs;
		flushOutHeaders(this.qs.getSelectors(), meta);
	}

	@Override
	public boolean hasNext() {
		boolean ret = baseIterator.hasNext();
		return ret;
	}

	@Override
	public IHeadersDataRow next() {
		Object data = baseIterator.next();
		Object[] retObject = null;

		// data will be a map for multi nodes being returned
		if(data instanceof Map) {
			Map<String, Object> mapData = (Map<String, Object>) data;
			int numSelectors = this.headerOrdering.length;
			retObject = new Object[numSelectors];

			for(int colIndex = 0; colIndex < numSelectors; colIndex++) {
				Object vertOrProp = mapData.get(this.headerOrdering[colIndex]);
				Object value = null;
				if (vertOrProp instanceof Vertex) {
					value = ((Vertex) vertOrProp).property(TinkerFrame.TINKER_NAME).value();
				} else {
					value = vertOrProp;
				}
				retObject[colIndex] = value;
			}
		} else {
			// not sure what will happen once we add group bys -> is this a map like above or different???
			// not sure what will happen once we add group bys -> is this a map like above or different???
			// not sure what will happen once we add group bys -> is this a map like above or different???
			// not sure what will happen once we add group bys -> is this a map like above or different???

			// for right now, assuming it is just a single vertex to return
			if(data instanceof Vertex) {
				TinkerVertex vertex = (TinkerVertex) data;
				retObject = new Object[]{vertex.property(TinkerFrame.TINKER_NAME).value()};
			} else {
				// some object to return
				retObject = new Object[]{data};
			}
		}

		HeadersDataRow nextData = new HeadersDataRow(this.headerAlias, this.header, retObject);
		return nextData;
	}

	/**
	 * Store the order of the headers to return
	 * @param selectors
	 */
	private void flushOutHeaders(List<IQuerySelector> selectors, OwlTemporalEngineMeta meta) {
		int numHeaders = selectors.size();
		this.header = new String[numHeaders];
		this.headerAlias = new String[numHeaders];
		this.headerOrdering = new String[numHeaders];
		int index = 0;
		for(IQuerySelector header : selectors) {
			if(header.getSelectorType() == IQuerySelector.SELECTOR_TYPE.COLUMN) {
				String alias = header.getAlias();
				String qsName = header.getQueryStructName();
				
				this.headerOrdering[index] = qsName;
				this.header[index] = alias;
				this.headerAlias[index] = getNodeAlias(meta, alias);
			} else if(header.getSelectorType() == IQuerySelector.SELECTOR_TYPE.FUNCTION) {
				List<IQuerySelector> innerSelectorList = ((QueryFunctionSelector) header).getInnerSelector();
				for(IQuerySelector innerSelector : innerSelectorList) {
					if(innerSelector.getSelectorType() == IQuerySelector.SELECTOR_TYPE.COLUMN) {
						String alias = innerSelector.getAlias();
						String qsName = innerSelector.getQueryStructName();
						
						this.headerOrdering[index] = qsName;
						this.header[index] = alias;
						this.headerAlias[index] = getNodeAlias(meta, alias);
					}
				}
			}
			index++;
		}
	}
	
	/**
	 * For some of the nodes that have not been given an alias
	 * If there is an implicit alias on it (a physical name that matches an existing name)
	 * We will use that
	 * @param node
	 * @return
	 */
	private String getNodeAlias(OwlTemporalEngineMeta meta, String node) {
		if(meta == null) {
			return node;
		}
		return meta.getPhysicalName(node);
	}

	public QueryStruct2 getQueryStruct() {
		return this.qs;
	}
}
