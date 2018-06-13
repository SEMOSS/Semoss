package prerna.ds;

import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.structure.Vertex;

import prerna.engine.api.IHeadersDataRow;
import prerna.om.HeadersDataRow;
import prerna.query.querystruct.SelectQueryStruct;
import prerna.query.querystruct.selectors.IQuerySelector;
import prerna.query.querystruct.selectors.QueryFunctionSelector;

public class TinkerHeadersDataRowIteratorMap implements Iterator<IHeadersDataRow> {

	private SelectQueryStruct qs;
	private GraphTraversal baseIterator;
	private String[] header;
	private String[] headerAlias;
	private String[] headerOrdering;
	private String[] types;
	private Map<String,String> nameMap;

	public TinkerHeadersDataRowIteratorMap(GraphTraversal composeIterator, SelectQueryStruct qs, Map<String, String> nameMap) {
		this.baseIterator = composeIterator;
		this.qs = qs;
		this.nameMap = nameMap;
		flushOutHeaders(this.qs.getSelectors(), null);
	}
	
	public TinkerHeadersDataRowIteratorMap(GraphTraversal composeIterator, SelectQueryStruct qs, OwlTemporalEngineMeta meta) {
		this.baseIterator = composeIterator;
		this.qs = qs;
		flushOutHeaders(this.qs.getSelectors(), meta);
	}

	@Override
	public boolean hasNext() {
		boolean ret = baseIterator.hasNext();
		if(!ret) {
			try {
				this.baseIterator.close();
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
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
					String node = this.headerOrdering[colIndex];
					String name = getNodeName(node);
					value = ((Vertex) vertOrProp).value(name);
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
				Vertex vertex = (Vertex) data;
				String node = this.headerOrdering[0];
				String name = getNodeName(node);
				retObject = new Object[]{vertex.value(name)};
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
		this.types = new String[numHeaders];
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
				this.types[index] = getTypes(meta, qsName);
			} else if(header.getSelectorType() == IQuerySelector.SELECTOR_TYPE.FUNCTION) {
				List<IQuerySelector> innerSelectorList = ((QueryFunctionSelector) header).getInnerSelector();
				for(IQuerySelector innerSelector : innerSelectorList) {
					if(innerSelector.getSelectorType() == IQuerySelector.SELECTOR_TYPE.COLUMN) {
						String alias = innerSelector.getAlias();
						String qsName = innerSelector.getQueryStructName();
						
						this.headerOrdering[index] = qsName;
						this.header[index] = alias;
						this.headerAlias[index] = getNodeAlias(meta, alias);
						this.types[index] = ((QueryFunctionSelector) header).getDataType();
					}
				}
			}
			index++;
		}
	}
	
	/**
	 * Get the types
	 * @return
	 */
	public String[] getTypes() {
		return this.types;
	}
	
	/**
	 * Get the type from the OWL if present
	 * @param meta
	 * @param qsName
	 * @return
	 */
	private String getTypes(OwlTemporalEngineMeta meta, String qsName) {
		if(meta == null) {
			return null;
		}
		return meta.getHeaderTypeAsString(qsName);
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

	private String getNodeName(String node) {
		if(this.nameMap != null) {
			if(this.nameMap.containsKey(node)) {
				return this.nameMap.get(node);
			}
		}
		return TinkerFrame.TINKER_NAME;
	}
	
	public SelectQueryStruct getQueryStruct() {
		return this.qs;
	}
}
