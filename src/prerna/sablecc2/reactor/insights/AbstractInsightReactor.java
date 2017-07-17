package prerna.sablecc2.reactor.insights;

import java.util.List;

import prerna.sablecc2.om.GenRowStruct;
import prerna.sablecc2.om.NounMetadata;
import prerna.sablecc2.om.PkslDataTypes;
import prerna.sablecc2.reactor.AbstractReactor;

public abstract class AbstractInsightReactor extends AbstractReactor {
	
	// used for running insights
	protected static final String ENGINE_KEY = "engine";
	protected static final String RDBMS_ID = "id";
	
	// used for saving a base insight
	protected static final String INSIGHT_NAME = "insightName";

	
	protected String getEngine() {
		// look at all the ways the insight panel could be passed
		// look at store if it was passed in
		GenRowStruct genericEngineGrs = this.store.getNoun(ENGINE_KEY);
		if(genericEngineGrs != null && !genericEngineGrs.isEmpty()) {
			return (String) genericEngineGrs.get(0);
		}
		
		// see if it is in the curRow
		// if it was passed directly in as a variable
		List<NounMetadata> stringNouns = this.curRow.getNounsOfType(PkslDataTypes.CONST_STRING);
		if(stringNouns != null && !stringNouns.isEmpty()) {
			return (String) stringNouns.get(0).getValue();
		}
		
		// well, you are out of luck
		return null;
	}
	
	/**
	 * This can either be passed specifically using the insightName key
	 * Or it is the second input in a list of values
	 * Save(engineName, insightName)
	 * @return
	 */
	protected String getInsightName() {
		// look at all the ways the insight panel could be passed
		// look at store if it was passed in
		GenRowStruct genericEngineGrs = this.store.getNoun(INSIGHT_NAME);
		if(genericEngineGrs != null && !genericEngineGrs.isEmpty()) {
			return (String) genericEngineGrs.get(0);
		}
		
		// see if it is in the curRow
		// if it was passed directly in as a variable
		// this will be the second input! (i.e. index 1)
		List<NounMetadata> stringNouns = this.curRow.getNounsOfType(PkslDataTypes.CONST_STRING);
		if(stringNouns != null && !stringNouns.isEmpty()) {
			return (String) stringNouns.get(1).getValue();
		}
		
		// well, you are out of luck
		return null;
	}
	
	protected int getRdbmsId() {
		// see if it was passed directly in with the lower case key ornaments
		GenRowStruct genericIdGrs = this.store.getNoun(RDBMS_ID);
		if(genericIdGrs != null && !genericIdGrs.isEmpty()) {
			return (int) genericIdGrs.get(0);
		}
		
		// see if it is in the curRow
		// if it was passed directly in as a variable
		List<NounMetadata> intNouns = this.curRow.getNounsOfType(PkslDataTypes.CONST_INT);
		if(intNouns != null && !intNouns.isEmpty()) {
			return (int) intNouns.get(0).getValue();
		}
		
		// well, you are out of luck
		return -1;
	}
	
}
