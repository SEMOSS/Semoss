package prerna.sablecc;

import java.util.Iterator;

import prerna.ds.QueryStruct;
import prerna.engine.api.IEngine;
import prerna.engine.impl.rdf.AbstractApiReactor;
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
		}
		this.qs.print();
		IQueryInterpreter interp = engine.getQueryInterpreter();
		interp.setQueryStruct(this.qs);
		String query = interp.composeQuery();

		this.put((String) getValue(PKQLEnum.API), query);
		this.put("RESPONSE", "success");
		this.put("STATUS", PKQLRunner.STATUS.SUCCESS);

		return null;
	}

	@Override
	public String explain() {
		String msg = "";
		msg += "NativeApiReactor";
		return msg;
	}
}
