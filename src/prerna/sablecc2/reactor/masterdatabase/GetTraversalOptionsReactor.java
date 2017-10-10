package prerna.sablecc2.reactor.masterdatabase;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Vector;

import prerna.nameserver.utility.MasterDatabaseUtility;
import prerna.sablecc2.om.GenRowStruct;
import prerna.sablecc2.om.NounMetadata;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.reactor.AbstractReactor;

public class GetTraversalOptionsReactor extends AbstractReactor {

	private static final String USING_LOGICAL = "logical";
	private static final String VALUES = "values";

	@Override
	public NounMetadata execute() {
		boolean usingLogical = usingLogical();
		List<String> logicals = null;
		if(usingLogical) {
			logicals = getLogicalInputNames();
		} else {
			List<String> conceptuals = getConceptualInputNames();
			if(!conceptuals.isEmpty()) {
				logicals = MasterDatabaseUtility.getAllLogicalNamesFromConceptualRDBMS(conceptuals);
			}
		}
		
		if(logicals != null && !logicals.isEmpty()) {
			Map<String, Object> traversalOptions = new HashMap<String, Object>();
			Map connectedConcepts = MasterDatabaseUtility.getConnectedConceptsRDBMS(logicals);
			Map<String, Object[]> conceptProperties = MasterDatabaseUtility.getConceptPropertiesRDBMS(logicals, null);
			traversalOptions.put("connectedConcepts", connectedConcepts);
			traversalOptions.put("connectedProperties", conceptProperties);
			return new NounMetadata(traversalOptions, PixelDataType.CUSTOM_DATA_STRUCTURE, PixelOperationType.TRAVERSAL_OPTIONS);
		}
		
		// if there are no traversal options
		// return empty map
		return new NounMetadata(new HashMap<String, Object>(), PixelDataType.CUSTOM_DATA_STRUCTURE, PixelOperationType.TRAVERSAL_OPTIONS);
	}

	
	/*
	 * Get the various input fields
	 */
	
	/**
	 * Get the values passed in assuming they are valid QS names
	 * @return
	 */
	private List<String> getConceptualInputNames() {
		GenRowStruct valuesGrs = this.store.getNoun(VALUES);
		if(valuesGrs != null && valuesGrs.size() > 0) {
			int numInputs = valuesGrs.size();
			List<String> inputs = new Vector<String>();
			for(int i = 0; i < numInputs; i++) {
				String input = valuesGrs.get(i).toString();
				// if FE is passing in a QS name
				if(input.contains("__")) {
					inputs.add(input.split("__")[1]);
				} else {
					inputs.add(input);
				}
			}
			return inputs;
		}
		
		int numInputs = this.curRow.size();
		List<String> inputs = new Vector<String>();
		for(int i = 0; i < numInputs; i++) {
			String input = this.curRow.get(i).toString();
			// if FE is passing in a QS name
			if(input.contains("__")) {
				inputs.add(input.split("__")[1]);
			} else {
				inputs.add(input);
			}
		}
		return inputs;
	}
	
	/**
	 * Get the values passed in assuming they are the logical names to use
	 * @return
	 */
	private List<String> getLogicalInputNames() {
		GenRowStruct valuesGrs = this.store.getNoun(VALUES);
		if(valuesGrs != null && valuesGrs.size() > 0) {
			int numInputs = valuesGrs.size();
			List<String> inputs = new Vector<String>();
			for(int i = 0; i < numInputs; i++) {
				inputs.add(valuesGrs.get(i).toString());
			}
			return inputs;
		}
		
		int numInputs = this.curRow.size();
		List<String> inputs = new Vector<String>();
		for(int i = 0; i < numInputs; i++) {
			inputs.add(this.curRow.get(i).toString());
		}
		return inputs;
	}
	
	private boolean usingLogical() {
		GenRowStruct logGrs = this.store.getNoun(USING_LOGICAL);
		if(logGrs != null) {
			if(logGrs.size() > 0) {
				return (boolean) logGrs.get(0);
			}
		}
		
		return false;
	}
}
