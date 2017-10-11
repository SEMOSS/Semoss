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
import prerna.query.querystruct.selectors.QueryMathSelector;

public class TinkerHeadersDataRowIterator2 implements Iterator<IHeadersDataRow> {

	private QueryStruct2 qs;
	private Iterator baseIterator;
	private String[] headerAlias;
	private String[] headerOrdering;

	public TinkerHeadersDataRowIterator2(Iterator composeIterator, QueryStruct2 qs) {
		this.baseIterator = composeIterator;
		this.qs = qs;
		flushOutHeaders(this.qs.getSelectors());
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
			}
		}

		HeadersDataRow nextData = new HeadersDataRow(this.headerAlias, retObject);
		return nextData;
	}

	/**
	 * Store the order of the headers to return
	 * @param selectors
	 */
	private void flushOutHeaders(List<IQuerySelector> selectors) {
		int numHeaders = selectors.size();
		this.headerAlias = new String[numHeaders];
		this.headerOrdering = new String[numHeaders];
		int index = 0;
		for(IQuerySelector header : selectors) {
			if(header.getSelectorType() == IQuerySelector.SELECTOR_TYPE.COLUMN) {
				String alias = header.getAlias();
				String qsName = header.getQueryStructName();
				
				this.headerOrdering[index] = qsName;
				this.headerAlias[index] = alias;
			} else if(header.getSelectorType() == IQuerySelector.SELECTOR_TYPE.MATH) {
				IQuerySelector innerSelector = ((QueryMathSelector) header).getInnerSelector();
				if(innerSelector.getSelectorType() == IQuerySelector.SELECTOR_TYPE.COLUMN) {
					String alias = innerSelector.getAlias();
					String qsName = innerSelector.getQueryStructName();
					
					this.headerOrdering[index] = qsName;
					this.headerAlias[index] = alias;
				}
			}
			index++;
		}
	}

	public QueryStruct2 getQueryStruct() {
		return this.qs;
	}
}
