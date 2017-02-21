package prerna.sablecc2.reactor.qs;

import prerna.ds.QueryStruct;
import prerna.sablecc2.reactor.AbstractReactor;

public abstract class QueryStructReactor extends AbstractReactor {

	QueryStruct qs;
	
	@Override
	public void In() {
		curNoun("all");
		if(qs == null) {
			qs = new QueryStruct();
		}
	}

	@Override
	public Object Out() {
		mergeUp();
		return parentReactor;
	}

	@Override
	protected void mergeUp() {
		if(parentReactor != null) {
			QueryStruct qs = createQueryStruct();
			
			//merge the query struct with the parent if we can
			if(parentReactor instanceof QueryStructReactor) {
				((QueryStructReactor) parentReactor).mergeQueryStruct(qs);
			} 
			
			//otherwise just push it
			else {
				//where though?
				updatePlan();
			}
			
		} else {
			updatePlan();
		}
	}

	@Override
	protected void updatePlan() {
		QueryStruct qs = createQueryStruct();
		
		//push this query struct to the planner
		QueryStruct mainQueryStruct = null;
		try {
			mainQueryStruct = (QueryStruct)this.planner.getProperty("QUERYSTRUCT", "QUERTYSTRUCT");
		}catch(Exception e) {
			
		}
		if(mainQueryStruct != null) {
			mainQueryStruct.merge(qs);
		} else {
			mainQueryStruct = qs;
		}
		this.planner.addProperty("QUERYSTRUCT", "QUERYSTRUCT", mainQueryStruct);
	}
	
	public void mergeQueryStruct(QueryStruct queryStruct) {
		if(qs == null) {
			qs = queryStruct;
		} else {
			qs.merge(queryStruct);
		}
	}
	
	abstract QueryStruct createQueryStruct();
}
