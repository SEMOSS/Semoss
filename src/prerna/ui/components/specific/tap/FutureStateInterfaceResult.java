package prerna.ui.components.specific.tap;

import java.util.Hashtable;

public class FutureStateInterfaceResult extends Hashtable<String, Object> {

	private boolean costTakenIntoConsideration = false;

	/*
	 * Determine possible actions from reviewing current interfaces
	 */
	public enum INTERFACE_TYPES {
		UPSTREAM_CONSUMER_FROM_DHMSM, 
		UPSTREAM_CONSUMER_FROM_DHMSM_AND_DECOMMISSION,
		DOWNSTREAM_CONSUMER_FROM_DHMSM, 
		DOWNSTREAM_CONSUMER_FROM_DHMSM_AND_DECOMMISSION,
		UPSTREAM_PROVIDER_TO_DHMSM, 
		UPSTREAM_PROVIDER_TO_DHMSM_AND_DECOMMISSION, 
		DOWNSTREAM_PROVIDER_TO_DHMSM, 
		DOWNSTREAM_PROVIDER_TO_DHMSM_AND_DECOMMISSION,
		DECOMMISSIONED, 
		STAY_AS_IS
		};
	
	public enum COST_TYPES {DIRECT, INDIRECT, NO_COST};

	/*
	 * set of valid keys in FutureStateInterfaceResult
	 */
	public static final String COST_TYPE = "costType";
	public static final String COMMENT = "comment";
	public static final String RECOMMENDATION = "recommendationType";
	public static final String LEGACY_UPSTREAM_SYSTEM = "upstreamSystem";
	public static final String LEGACY_DOWNSTREAM_SYSTEM = "downstreamSystem";

//	public static final String NEW_INTERFACE = "newInterface";
//	public static final String REMOVED_INTERFACE = "removedInterface";
	
	@Override
	public Object put(String key, Object value) {
		// ensure keys are those defined above
		if(!key.equals(COST_TYPE) && !key.equals(COMMENT) && !key.equals(RECOMMENDATION) && !key.equals(LEGACY_UPSTREAM_SYSTEM) && !key.equals(LEGACY_DOWNSTREAM_SYSTEM)) {
			throw new IllegalArgumentException("Key is not valid.  Please see keys in FutureStateInterfaceResult for valid keys");
		}
		return super.put(key, value);
	}
	
	public boolean isCostTakenIntoConsideration() {
		return costTakenIntoConsideration;
	}

	public void setCostTakenIntoConsideration(boolean costTakenIntoConsideration) {
		this.costTakenIntoConsideration = costTakenIntoConsideration;
	}
}
