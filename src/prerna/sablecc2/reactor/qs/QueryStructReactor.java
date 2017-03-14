package prerna.sablecc2.reactor.qs;

import java.util.List;

import prerna.ds.QueryStruct2;
import prerna.ds.QueryStructSelector;
import prerna.sablecc2.om.NounMetadata;
import prerna.sablecc2.reactor.AbstractReactor;

public abstract class QueryStructReactor extends AbstractReactor {

	QueryStruct2 qs;
	
	@Override
	public void In() {
		curNoun("all");
		if(qs == null) {
			qs = new QueryStruct2();
		}
	}

	@Override
	public Object Out() {
		mergeUp();
		return parentReactor;
	}

	@Override
	protected void mergeUp() {
//		if(parentReactor != null) {
			QueryStruct2 qs = createQueryStruct();
			
			if(this.planner.hasProperty("RESULT", "RESULT")) {
				NounMetadata result = (NounMetadata) this.planner.getProperty("RESULT", "RESULT");
				if(result.getNounName().equals("QUERYSTRUCT")) {
					QueryStruct2 storedResult = (QueryStruct2)result.getValue();
					storedResult.merge(qs);
				} else {
					NounMetadata queryStruct = new NounMetadata(qs, "QUERYSTRUCT");
					this.planner.addProperty("RESULT", "RESULT", queryStruct);
				}
			} else {
				NounMetadata queryStruct = new NounMetadata(qs, "QUERYSTRUCT");
				this.planner.addProperty("RESULT", "RESULT", queryStruct);
			}
			
//			//merge the query struct with the parent if we can
//			if(parentReactor instanceof QueryStructReactor) {
//				((QueryStructReactor) parentReactor).mergeQueryStruct(qs);
//			} 
//			
//			//otherwise just push it
//			else {
//				//where though?
//				updatePlan();
//			}
			
//		} else {
//			updatePlan();
//		}
	}

	@Override
	protected void updatePlan() {
//		QueryStruct2 qs = createQueryStruct();
		
//		//push this query struct to the planner
//		QueryStruct2 mainQueryStruct = null;
//		try {
//			mainQueryStruct = (QueryStruct2)this.planner.getProperty("QUERYSTRUCT", "QUERTYSTRUCT");
//		}catch(Exception e) {
//			
//		}
//		if(mainQueryStruct != null) {
//			mainQueryStruct.merge(qs);
//		} else {
//			mainQueryStruct = qs;
//		}
//		this.planner.addProperty("QUERYSTRUCT", "QUERYSTRUCT", mainQueryStruct);
	}
	
	public void mergeQueryStruct(QueryStruct2 queryStruct) {
		if(qs == null) {
			qs = queryStruct;
		} else {
			qs.merge(queryStruct);
		}
	}
	
	public void setAs(String [] asName) {
		List<QueryStructSelector> selectors = qs.getSelectors();
		for(int i = 0; i < asName.length; i++) {
			selectors.get(i).setAlias(asName[i]);
		}
	}
	
	abstract QueryStruct2 createQueryStruct();
}
