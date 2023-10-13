package prerna.reactor.planner.graph;

import java.util.List;
import java.util.Map;
import java.util.Vector;

import prerna.reactor.BaseJavaRuntime;
import prerna.reactor.PixelPlanner;
import prerna.reactor.tax.TaxUtility;
import prerna.sablecc2.om.GenRowStruct;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.nounmeta.NounMetadata;

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
		List<String> pkslsToAdd = getPkslStrings(pksls);		
		
		PixelPlanner basePlanner = getPlanner();
		Class<BaseJavaRuntime> javaRunClass = (Class<BaseJavaRuntime>) basePlanner.getProperty("RUN_CLASS", "RUN_CLASS");
		RuntimeJavaClassBuilder builder = new RuntimeJavaClassBuilder();
		builder.setSuperClass(javaRunClass);
		builder.addEquations(pkslsToAdd);
		Class<BaseJavaRuntime> updatedRunClass = builder.generateUpdateClass();
		PixelPlanner updatedPlan = new PixelPlanner();
		updatedPlan.addProperty("RUN_CLASS", "RUN_CLASS", updatedRunClass);
		return new NounMetadata(updatedPlan, PixelDataType.PLANNER);
	}
	
	/**
	 * Get the Base Plan passed as the second Parameter
	 * 
	 * @return
	 */
	protected PixelPlanner getPlanner() {
		GenRowStruct allNouns = getNounStore().getNoun(PixelDataType.PLANNER.toString());
		if (allNouns != null && allNouns.size() > 0) {
			Object firstParam = allNouns.get(0);
			if (firstParam != null) {
				PixelPlanner basePlan = (PixelPlanner) firstParam;
				return basePlan;
			}
		}
		return null;
	}
	
	/**
	 * This method returns the set of pksls to add
	 * It assumes the FE is passing the pksls using the alias
	 * and we convert that to the hashcode
	 * @param pksls
	 * @return
	 */
	private List<String> getPkslStrings(GenRowStruct pksls) {
		// in the grs
		// these strings have the alias
		// we need to convert them to the hashcode
		
		// keep the the list of the component pksls so we can 
		// substitute the first index from alias to hashcode
		List<String[]> componentPksls = new Vector<String[]>();
		
		// keep a list of the aliases
		// this is so we can run fewer queries on the engines
		List<String> aliasList = new Vector<String>();
		
		// iterate through the pksls being updated
		int numPksls = pksls.size();
		for(int i = 0; i < numPksls; i++) {
			String aliasPksl = pksls.get(i).toString();
			
			// split on the equals and add it ot the list
			String[] aliasBreak = aliasPksl.split("=");
			componentPksls.add(aliasBreak);
			
			// add the alias to the list
			aliasList.add(aliasBreak[0].trim());
		}
		
		// get the proper hash for each alias
		Map<String, String> aliasToHashMap = TaxUtility.mapAliasToHash(aliasList);

		// reconstruct the pksls passed using the hashcode
		List<String> pkslsToAdd = new Vector<String>();
		for(int i = 0; i < numPksls; i++) {
			String[] aliasBreak = componentPksls.get(i);
			// replace the alias with the hashcode
			String newPkslString = aliasToHashMap.get(aliasBreak[0].trim()) + " = " + aliasBreak[1];
			if(!newPkslString.endsWith(";")) {
				newPkslString = newPkslString + ";";
			}
			pkslsToAdd.add(newPkslString);
		}
		
		return pkslsToAdd;
	}
	
}
