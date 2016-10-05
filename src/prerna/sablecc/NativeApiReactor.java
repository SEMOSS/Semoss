package prerna.sablecc;

import java.util.Iterator;
import java.util.Map;
import java.util.Set;

import prerna.ds.QueryStruct;
import prerna.ds.nativeframe.NativeFrame;
import prerna.engine.api.IEngine;
import prerna.engine.api.IRawSelectWrapper;
import prerna.engine.impl.rdf.AbstractApiReactor;
import prerna.rdf.engine.wrappers.WrapperManager;
import prerna.rdf.query.builder.IQueryInterpreter;
import prerna.util.Utility;

public class NativeApiReactor extends AbstractApiReactor {

	/*
	 * This class is exactly the same as the QueryAPI class except for one
	 * difference The QueryAPI gets the query and runs it to store an iterator
	 * This class gets the query and returns the query
	 */

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
		this.qs.print();
		
		//compose the query associated with the querystruct
		IQueryInterpreter interp = engine.getQueryInterpreter();
		interp.setQueryStruct(this.qs);
		String query = interp.composeQuery();

		//merge the query struct to the frame's querystruct if it is using the same engine
		NativeFrame nativeFrame = (NativeFrame)myStore.get("G");
		if(nativeFrame.getEngineName() == null) {
			nativeFrame.setConnection(engine.getEngineName());
		}
		
		//merge the query struct to the frame's querystruct if it is using the same engine
		if(nativeFrame.getEngineName().equals(engine.getEngineName())) {
			nativeFrame.mergeQueryStruct(this.qs);
			this.put((String) getValue(PKQLEnum.API), query);
		} 
		
		//else we want to convert native frame to an h2 frame since we are traversing across db's
		else {
			this.qs.print();
			Iterator thisIterator = null;
			// in order to support legacy GDM insights
			// need to determine if we should get a select wrapper or a cheater wrapper
			// we grab this boolean from the AbstractApiReactor
			if(this.useCheater) {
				thisIterator = WrapperManager.getInstance().getChWrapper(engine, query);
			} else {
				// return the raw wrapper
				thisIterator = WrapperManager.getInstance().getRawWrapper(engine, query);
			}
			
			this.put((String) getValue(PKQLEnum.API), thisIterator);
			
			interp.clear();
			interp.setPerformCount(true);
			query = interp.composeQuery();
			IRawSelectWrapper countIt = WrapperManager.getInstance().getRawWrapper(engine, query);
			if(countIt.hasNext()) {
				Object numCells = countIt.next().getValues()[0];
				System.out.println("QUERY CONTAINS NUM_CELLS = " + numCells);
				
				this.put("QUERY_NUM_CELLS", numCells);
			}
		}
		
		//store values
		this.put("RESPONSE", "success");
		this.put("STATUS", PKQLRunner.STATUS.SUCCESS);

		return null;
	}
}
