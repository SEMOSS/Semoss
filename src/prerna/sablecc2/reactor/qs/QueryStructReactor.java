package prerna.sablecc2.reactor.qs;

import java.util.List;

import prerna.ds.QueryStruct2;
import prerna.ds.QueryStructSelector;
import prerna.sablecc2.om.GenRowStruct;
import prerna.sablecc2.om.NounMetadata;
import prerna.sablecc2.om.PkslDataTypes;
import prerna.sablecc2.reactor.AbstractReactor;

public abstract class QueryStructReactor extends AbstractReactor {

	QueryStruct2 qs;
	
	@Override
	public void In() {
		curNoun("all");
		
	}

	@Override
	public Object Out() {
		init();
		return this.parentReactor;
	}

	@Override
	public Object execute() {
		QueryStruct2 qs = createQueryStruct();
		NounMetadata noun = new NounMetadata(qs, PkslDataTypes.QUERY_STRUCT);
		return noun;
	}
	@Override
	protected void mergeUp() {

	}

	@Override
	protected void updatePlan() {

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
	
	private void init() {
		GenRowStruct qsInputParams = getNounStore().getNoun(PkslDataTypes.QUERY_STRUCT.toString());
		if(qsInputParams != null) {
			int numInputs = qsInputParams.size();
			for(int inputIdx = 0; inputIdx < numInputs; inputIdx++) {
				NounMetadata qsNoun = (NounMetadata)qsInputParams.get(inputIdx);
				mergeQueryStruct((QueryStruct2)qsNoun.getValue());
			}
		}
		
		if(qs == null) {
			qs = new QueryStruct2();
		}
	}
	
	abstract QueryStruct2 createQueryStruct();
}
