package prerna.sablecc2.reactor.qs;

import java.util.List;

import prerna.ds.querystruct.QueryStruct2;
import prerna.ds.querystruct.QueryStructSelector;
import prerna.sablecc2.om.GenRowStruct;
import prerna.sablecc2.om.NounMetadata;
import prerna.sablecc2.om.PkslDataTypes;
import prerna.sablecc2.reactor.AbstractReactor;

/**
 * 
 * This is the base class for any reactor responsible for building a querystruct
 * 
 * The design of this class is as such:
 * 		This class holds a query struct, responsible for grabbing input and passing output
 * 		any subclass is only responsible for building the query struct through method createQueryStruct()
 * 		Ex:
 * 			SelectReactor overrides createQueryStruct() build on top of the existing query struct with only Selectors
 * 			JoinReactor overrides createQueryStruct() builds on top of the existing query struct with only joins
 * 
 * 			QueryStructReactor is responsible for grabbing any input querystructs and passing the output
 *
 */
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
		//build the query struct
		QueryStruct2 qs = createQueryStruct();
		
		//create the output and return
		NounMetadata noun = new NounMetadata(qs, PkslDataTypes.QUERY_STRUCT);
		return noun;
	}
	
	@Override
	protected void mergeUp() {

	}

	@Override
	protected void updatePlan() {

	}
	
	//method to merge an outside query struct with this query struct
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
	
	//initialize the reactor with its necessary inputs
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
