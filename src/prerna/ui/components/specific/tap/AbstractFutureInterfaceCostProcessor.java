package prerna.ui.components.specific.tap;

import java.util.Map;

import prerna.engine.api.IDatabaseEngine;

public abstract class AbstractFutureInterfaceCostProcessor {

	/**
	 * Define set of appropriate cost frameworks that this interface supports
	 */
	public enum COST_FRAMEWORK {SOA, P2P}

	/**
	 * Returns a Map with the cost information for the future interface
	 * @return
	 */
	public abstract Map<String, Double> calculateCost(String dataObject, String system, String tag);

	/**
	 * Run queries to get cost information at appropriate level
	 */
	public abstract void getCostInfo();

	/**
	 * Define required engines
	 * @param engines
	 */
	public abstract void setCostEngines(IDatabaseEngine[] engines);

	/**
	 * Set the cost framework
	 * @param framework
	 */
	public abstract void setCostFramework(COST_FRAMEWORK framework);
	
}
