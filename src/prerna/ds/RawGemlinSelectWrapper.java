package prerna.ds;

import java.util.List;
import java.util.Map;

import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.structure.Vertex;

import prerna.algorithm.api.SemossDataType;
import prerna.engine.api.IEngine;
import prerna.engine.api.IHeadersDataRow;
import prerna.engine.api.IRawSelectWrapper;
import prerna.engine.impl.rdbms.RDBMSNativeEngine;
import prerna.engine.impl.tinker.TinkerEngine;
import prerna.om.HeadersDataRow;
import prerna.query.interpreters.GremlinInterpreter;
import prerna.query.querystruct.SelectQueryStruct;
import prerna.query.querystruct.selectors.IQuerySelector;
import prerna.query.querystruct.selectors.QueryColumnSelector;
import prerna.query.querystruct.selectors.QueryFunctionSelector;
import prerna.rdf.engine.wrappers.AbstractWrapper;
import prerna.test.TestUtilityMethods;
import prerna.util.DIHelper;

public class RawGemlinSelectWrapper extends AbstractWrapper implements IRawSelectWrapper {

	private GremlinInterpreter interp;
	private SelectQueryStruct qs;
	private Map<String,String> nameMap;
	private OwlTemporalEngineMeta meta;

	private GraphTraversal baseIterator;
	
	public RawGemlinSelectWrapper(GremlinInterpreter interp, SelectQueryStruct qs) {
		this.interp = interp;
		this.qs = qs;
		this.nameMap = interp.getNameMap();
	}
	
	public RawGemlinSelectWrapper(GremlinInterpreter interp, SelectQueryStruct qs, OwlTemporalEngineMeta meta) {
		this.interp = interp;
		this.qs = qs;
	}
	
	@Override
	public void execute() {
		this.interp.setQueryStruct(this.qs);
		this.baseIterator = this.interp.composeIterator();
		
		List<IQuerySelector> selectors = this.qs.getSelectors();
		this.numColumns = selectors.size();
		this.rawHeaders = new String[numColumns];
		this.headers = new String[numColumns];
		this.types = new SemossDataType[numColumns];
		
		int index = 0;
		for(IQuerySelector header : selectors) {
			if(header.getSelectorType() == IQuerySelector.SELECTOR_TYPE.COLUMN) {
				String alias = header.getAlias();
				String qsName = header.getQueryStructName();

				this.rawHeaders[index] = qsName;
				this.headers[index] = getNodeAlias(meta, alias);
				this.types[index] = getTypes(meta, qsName);
			}
			else if(header.getSelectorType() == IQuerySelector.SELECTOR_TYPE.FUNCTION) {
				List<IQuerySelector> innerSelectorList = ((QueryFunctionSelector) header).getInnerSelector();
				for(IQuerySelector innerSelector : innerSelectorList) {
					if(innerSelector.getSelectorType() == IQuerySelector.SELECTOR_TYPE.COLUMN) {
						String alias = innerSelector.getAlias();
						String qsName = innerSelector.getQueryStructName();
						
						this.rawHeaders[index] = qsName;
						this.headers[index] = getNodeAlias(meta, alias);
						this.types[index] = SemossDataType.convertStringToDataType(((QueryFunctionSelector) header).getDataType());
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

	/**
	 * Get the type from the OWL if present
	 * @param meta
	 * @param qsName
	 * @return
	 */
	private SemossDataType getTypes(OwlTemporalEngineMeta meta, String qsName) {
		if(meta == null) {
			return null;
		}
		return meta.getHeaderTypeAsEnum(qsName);
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
			retObject = new Object[this.numColumns];

			for(int colIndex = 0; colIndex < this.numColumns; colIndex++) {
				Object vertOrProp = mapData.get(this.rawHeaders[colIndex]);
				Object value = null;
				if (vertOrProp instanceof Vertex) {
					String node = this.rawHeaders[colIndex];
					String name = getNodeName(node);
					value = ((Vertex) vertOrProp).value(name);
				} else {
					value = vertOrProp;
				}
				retObject[colIndex] = value;
			}
		} else {
			// not sure what will happen once we add group bys -> is this a map like above or different???

			// for right now, assuming it is just a single vertex to return
			if(data instanceof Vertex) {
				Vertex vertex = (Vertex) data;
				String node = this.rawHeaders[0];
				String name = getNodeName(node);
				retObject = new Object[]{vertex.value(name)};
			} else {
				// some object to return
				retObject = new Object[]{data};
			}
		}

		HeadersDataRow nextData = new HeadersDataRow(this.headers, this.rawHeaders, retObject);
		return nextData;
	}
	
	private String getNodeName(String node) {
		if(this.nameMap != null) {
			if(this.nameMap.containsKey(node)) {
				return this.nameMap.get(node);
			}
		}
		return TinkerFrame.TINKER_NAME;
	}
	
	
	@Override
	public void cleanUp() {
		try {
			baseIterator.close();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	@Override
	public long getNumRows() {
		if(this.numRows == 0) {
			GremlinInterpreter interp = this.interp.copy();
			GraphTraversal it = interp.composeIterator();
			
			GraphTraversal<Vertex, Long> numValues = it.count();
			try {
				if(numValues.hasNext()) {
					this.numRows = numValues.next();
				}
			} finally {
				try {
					numValues.close();
				} catch (Exception e) {
					e.printStackTrace();
				}
			}
		}
		return this.numRows;
	}

	@Override
	public long getNumRecords() {
		return getNumRows() * this.numColumns;
	}

	@Override
	public void reset() {
		cleanUp();
		this.interp.reset();
		this.baseIterator = this.interp.composeIterator();
	}
	
	@Override
	public String[] getHeaders() {
		return this.rawHeaders;
	}

	@Override
	public SemossDataType[] getTypes() {
		return this.types;
	}

	///////////////////////////////////////////////////////////////////
	///////////////////////////////////////////////////////////////////
	///////////////////////////////////////////////////////////////////

	/*
	 * Main for testing
	 */
	
	public static void main(String[] args) throws Exception {
		TestUtilityMethods.loadDIHelper("C:\\workspace\\Semoss_Dev\\RDF_Map.prop");
		{
			String engineProp = "C:\\workspace\\Semoss_Dev\\db\\LocalMasterDatabase.smss";
			IEngine coreEngine = new RDBMSNativeEngine();
			coreEngine.setEngineId("LocalMasterDatabase");
			coreEngine.openDB(engineProp);
			coreEngine.setEngineId("LocalMasterDatabase");
			DIHelper.getInstance().setLocalProperty("LocalMasterDatabase", coreEngine);
		}
		
	
		String testEngine = "TinkerThis__cc2a91eb-548d-4970-91c3-7a043b783841";
		String engineProp = "C:\\workspace\\Semoss_Dev\\db\\" + testEngine + ".smss";
		TinkerEngine coreEngine = new TinkerEngine();
		coreEngine.openDB(engineProp);
		DIHelper.getInstance().setLocalProperty(testEngine, coreEngine);
		
		
		GremlinInterpreter interp = new GremlinInterpreter(coreEngine.getGraph().traversal(), 
				coreEngine.getTypeMap(), coreEngine.getNameMap());
		
		
		SelectQueryStruct qs = new SelectQueryStruct();
		qs.addSelector(new QueryColumnSelector("Title"));
		qs.addSelector(new QueryColumnSelector("Title__MovieBudget"));
		qs.addSelector(new QueryColumnSelector("Studio"));
		qs.addRelation("Title", "Studio", "inner.join");
		
		RawGemlinSelectWrapper it = new RawGemlinSelectWrapper(interp, qs);
		it.execute();
		System.out.println(it.getNumRecords());
	}

	@Override
	public void setQuery(String query) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public String getQuery() {
		// TODO Auto-generated method stub
		return null;
	}
	
	@Override
	public void setEngine(IEngine engine) {
		// TODO Auto-generated method stub
		
	}
}
