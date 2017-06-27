package prerna.sablecc2.reactor.planner.graph;

import java.util.List;
import java.util.Vector;

import prerna.sablecc2.om.GenRowStruct;
import prerna.sablecc2.om.NounMetadata;
import prerna.sablecc2.om.PkslDataTypes;
import prerna.sablecc2.reactor.BaseJavaRuntime;
import prerna.sablecc2.reactor.PKSLPlanner;

public class UpdateGraphPlannerReactor2 extends AbstractPlannerReactor {

	public static final String PKSL_NOUN = "pksls";
	public static final String STORE_NOUN = "store";
	
	@Override
	public NounMetadata execute()
	{
//		long start = System.currentTimeMillis();
		
		// grab all the pksls
		GenRowStruct pksls = this.store.getNoun(PKSL_NOUN);
		
		// store them in a list
		// and also keep a builder with all the executions
		List<String> pkslsToAdd = new Vector<String>();
		int numPksls = pksls.size();
		PKSLPlanner basePlanner = getPlanner();
		BaseJavaRuntime javaRunClass = (BaseJavaRuntime) basePlanner.getProperty("RUN_CLASS", "RUN_CLASS");
		for(int i = 0; i < numPksls; i++) {
			pkslsToAdd.add(pksls.get(i).toString());
		}
		RuntimeJavaClassBuilder builder = new RuntimeJavaClassBuilder();
		builder.setSuperClass(javaRunClass.getClass());
		builder.addEquations(pkslsToAdd);
		BaseJavaRuntime updatedRunClass = builder.buildUpdateClass();
		PKSLPlanner updatedPlan = new PKSLPlanner();
		updatedPlan.addProperty("RUN_CLASS", "RUN_CLASS", updatedRunClass);
		return new NounMetadata(updatedPlan, PkslDataTypes.PLANNER);
	}
	
	/**
	 * Get the Base Plan passed as the second Parameter
	 * 
	 * @return
	 */
	protected PKSLPlanner getPlanner() {

		GenRowStruct allNouns = getNounStore().getNoun(PkslDataTypes.PLANNER.toString());
		if (allNouns != null && allNouns.size() > 0) {
			Object firstParam = allNouns.get(0);
			if (firstParam != null) {
				PKSLPlanner basePlan = (PKSLPlanner) firstParam;
				return basePlan;
			}
		}
		return null;
	}
}
