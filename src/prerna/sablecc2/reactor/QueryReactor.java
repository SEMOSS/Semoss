package prerna.sablecc2.reactor;

import java.util.Enumeration;
import java.util.Hashtable;
import java.util.Vector;

import prerna.ds.QueryStruct;
import prerna.sablecc2.om.Filter;
import prerna.sablecc2.om.GenRowStruct;
import prerna.sablecc2.om.Join;
import prerna.sablecc2.om.NounMetadata;

public class QueryReactor extends AbstractReactor {

	public QueryReactor() {
		setName("Query");
	}
	@Override
	public void In() {
		this.defaultOutputAlias = new String[]{"QUERY_STRUCT"};
		curNoun("all");
	}

	@Override
	public Object Out() {
		updatePlan();
		if(this.parentReactor != null) {
			return parentReactor;
		}
		
		return null;
		
	}


	@Override
	protected void mergeUp() {
		if(parentReactor != null) {
			buildQueryStruct();
			parentReactor.setProp("qs", getProp("qs"));
			parentReactor.setProp("db", getProp("db"));
			GenRowStruct joins = store.nounRow.get("merge");
			if(joins != null) {
//				GenRowStruct merge = parentReactor.getNounStore().makeNoun("merge");
				parentReactor.getNounStore().addNoun("merge", joins);
			}
		}
	}

	@Override
	protected void updatePlan() {
		
		getType();
		Enumeration <String> keys = store.nounRow.keys();
		
		String reactorOutput = reactorName;
		
		while(keys.hasMoreElements())
		{
			String singleKey = keys.nextElement();
			GenRowStruct struct = store.nounRow.get(singleKey);
			Vector <String> inputs = struct.getAllColumns();
			
			// need a better way to do it
			if(asName == null)
				reactorOutput = reactorOutput + "_" + struct.getColumns();
	
			// find if code exists
			if(!propStore.containsKey("CODE"))
			{
				if(inputs.size() > 0)
					planner.addInputs(signature, inputs, type);
			}
		}
		//first lets assume 0 inputs...will change that later
		//assume 2 outputs from this reactor
			//1. the query struct OR the query itself (Let's assume just query struct for now)
			//2. the engine (optional)
		
		//1. generate the outputs
			//a. build the query struct
			//b. stores the query struct on this reactor's prop store
		buildQueryStruct();
		
		//2. establishes the references by which those objects are available elsewhere
		addOutputLinksToPlanner();
				
		//3. stores the output objects to the planner
		storeOutputsToPlanner();		
	}
	
	//upload the outputs to the planner
	private void storeOutputsToPlanner() {
		Hashtable<String, Object> outputStore = new Hashtable<>();
		outputStore.put("qs", getProp("qs"));
		if(getProp("db") != null) {
			outputStore.put("db", getProp("db"));
		}
		this.planner.addProperty(asName[0], "STORE", outputStore);
	}
	
	//build this reactors outputs and store
	private QueryStruct buildQueryStruct() {
		//consolidate data here and merge up a query struct
		QueryStruct qs = new QueryStruct();
		
		for(String nextNoun : store.nounRow.keySet()) {
			switch(nextNoun) {
				//Selectors
				case "s": {
					GenRowStruct selectors = store.nounRow.get(nextNoun);
					for(int i = 0; i < selectors.size(); i++) {
						String nextSelector = (String)selectors.get(i);
						if(nextSelector.contains("__")) {
							String concept = nextSelector.substring(0, nextSelector.indexOf("__"));
							String property = nextSelector.substring(nextSelector.indexOf("__")+2);
							qs.addSelector(concept, property);
						}
						else {
							qs.addSelector(nextSelector, null);
						}
					}
					break;
				}
				//Filters
				case "f": {
					//look for filters here
					GenRowStruct filters = store.nounRow.get(nextNoun);
					for(int i = 0; i < filters.size(); i++) {
						Filter nextFilter = (Filter)filters.get(i);
						qs.addFilter(nextFilter.getSelector(), nextFilter.getComparator(), nextFilter.getValues().vector);
					}
					break;
				}
				//Joins
				case "joins": {
					//database joins
					GenRowStruct joins = store.nounRow.get(nextNoun);
					for(int i = 0; i < joins.size(); i++) {
						if(joins.get(i) instanceof Join) {
							Join join = (Join)joins.get(i);
							qs.addRelation(join.getSelector(), join.getQualifier(), join.getJoinType());
						}
					}
					break;
				} 
			}
			
		}
		String database = (String) this.getProp("db");
		if(database != null) {
			System.out.println(database);
		}
		Integer limit = (Integer)this.getProp("limit");
		if(limit != null) {
			qs.setLimit(limit);
		}
		Integer offset = (Integer)this.getProp("offset");
		if(offset != null) {
			qs.setOffSet(offset);
		}
		setProp("qs", qs);
		return qs;
	}
	
	@Override
	public Vector<NounMetadata> getInputs() {
		return null;
	}
}
