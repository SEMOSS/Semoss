package prerna.sablecc;

import java.util.Iterator;
import java.util.Map;
import java.util.Set;

import org.apache.tinkerpop.gremlin.structure.Vertex;

import prerna.ds.GremlinInterpreter;
import prerna.ds.QueryStruct;
import prerna.ds.TinkerQueryInterpreter;
import prerna.engine.api.IEngine;
import prerna.engine.impl.rdf.AbstractApiReactor;
import prerna.engine.impl.tinker.TinkerEngine;
import prerna.rdf.engine.wrappers.WrapperManager;
import prerna.rdf.query.builder.IQueryInterpreter;
import prerna.sablecc.meta.IPkqlMetadata;
import prerna.util.Utility;

public class TinkerQueryApiReactor extends AbstractApiReactor {

	@Override
	public Iterator process() {
		super.process();

		// if the engine is in DIHelper, it will grab it
		// otherwise, it will load the engine using the smss and return it
		// we grab the engine from the AbstractApiReactor
		IEngine engine = Utility.getEngine(this.engine);

		// logic that if a person is trying to query an engine
		// and if the query struct is empty
		// just pull all the information from the engine
		// we grab the qs from the AbstractApiReactor
		if (this.qs.isEmpty()) {
			// we cannot override the existing query struct since
			// it is used in APIReactor to get the edgeHash
			// so we need to update the existing one to get the new values
			// TODO should expose this on the QueryStruct object itself
			QueryStruct newQs = engine.getDatabaseQueryStruct();
			this.qs.selectors = newQs.selectors;
			this.qs.relations = newQs.relations;
			this.qs.andfilters = newQs.andfilters;

			// need to set a new edge hash using this information!!!
			// since the abstract api reactor will set one where the edge hash
			// will be empty
			Map<String, Set<String>> edgeHash = this.qs.getReturnConnectionsHash();
			// we store the edge hash in myStore
			this.put("EDGE_HASH", edgeHash);
		}
		// this.qs.print();

//		Iterator thisIterator = interp.composeIterator();
		((TinkerEngine) engine).setQueryStruct(this.qs);
		Iterator thisIterator = WrapperManager.getInstance().getRawWrapper(engine, null);

		this.put((String) getValue(PKQLEnum.API), thisIterator);
		this.put("RESPONSE", "success");
		this.put("STATUS", PKQLRunner.STATUS.SUCCESS);
		
		return null;

	}

	@Override
	public IPkqlMetadata getPkqlMetadata() {
		// TODO Auto-generated method stub
		return null;
	}

}
