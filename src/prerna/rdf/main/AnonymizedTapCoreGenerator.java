package prerna.rdf.main;

import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Vector;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

import prerna.engine.api.IHeadersDataRow;
import prerna.engine.api.IRawSelectWrapper;
import prerna.engine.impl.rdf.BigDataEngine;
import prerna.query.querystruct.SelectQueryStruct;
import prerna.query.querystruct.selectors.QueryColumnSelector;
import prerna.rdf.engine.wrappers.WrapperManager;
import prerna.test.TestUtilityMethods;

public class AnonymizedTapCoreGenerator {

	private static final Gson gson = new GsonBuilder().setPrettyPrinting().create();

	public static void main(String[] args) throws Exception {
		TestUtilityMethods.loadAll("C:\\workspace\\Semoss_Dev\\RDF_Map.prop");

		List<String> systemReplacementOrder = new Vector<String>();
		Map<String, String> systemMapping = new HashMap<String, String>();

		{
			String smss = "C:\\workspace\\Semoss_Dev\\db\\TAP_Core_Data__133db94b-4371-4763-bff9-edf7e5ed021b.smss";
			BigDataEngine engine = new BigDataEngine();
			engine.openDB(smss);

			// get a list of all the systems
			SelectQueryStruct qs = new SelectQueryStruct();
			qs.addSelector(new QueryColumnSelector("System"));
			qs.addOrderBy("System");

			int counter = 1;
			IRawSelectWrapper wrapper = WrapperManager.getInstance().getRawWrapper(engine, qs);
			while(wrapper.hasNext()) {
				IHeadersDataRow data = wrapper.next();
				Object[] row = data.getValues();
				String system = row[0].toString();
				systemMapping.put(system, "System" + counter);
				counter++;

				// keep track of all systems
				// will order this so we know what to replace when
				systemReplacementOrder.add(system);
			}

			//System.out.println(systemMapping.size());
			//System.out.println(gson.toJson(systemMapping));

			// order the systems from largest to smallest
			systemReplacementOrder.sort(new Comparator<String>() {
				@Override
				public int compare(String o1, String o2) {
					if(o1.length() > o2.length()) {
						return -1;
					} else if(o1.length() < o2.length()) {
						return 1;
					}
					return 0;
				}
			});

			//System.out.println(gson.toJson(systemReplacementOrder));

			runReplacementForEngine(engine, systemReplacementOrder, systemMapping);
		}
		
		{
			String smss = "C:\\workspace\\Semoss_Dev\\db\\TAP_Site_Data__eed12b32-bc38-4718-ab73-c0c78480c174.smss";
			BigDataEngine engine = new BigDataEngine();
			engine.openDB(smss);
			runReplacementForEngine(engine, systemReplacementOrder, systemMapping);
		}
		
		{
			String smss = "C:\\workspace\\Semoss_Dev\\db\\TAP_Portfolio__4254569c-3e78-4d62-8a07-1f786edf71e6.smss";
			BigDataEngine engine = new BigDataEngine();
			engine.openDB(smss);
			runReplacementForEngine(engine, systemReplacementOrder, systemMapping);
		}
	}

	private static void runReplacementForEngine(BigDataEngine engine, List<String> systemReplacementOrder, Map<String, String> systemMapping) {
		List<Object[]> removeTriples = new Vector<Object[]>();
		List<Object[]> addTriples = new Vector<Object[]>();

		System.out.println("Staring execution for " + engine.getEngineName());
		int counter = 0;

		String query = "select ?s ?p ?o where {"
				+ "{?s ?p ?o}"
				+ "}";

		IRawSelectWrapper wrapper = WrapperManager.getInstance().getRawWrapper(engine, query);
		while(wrapper.hasNext()) {
			IHeadersDataRow data = wrapper.next();
			Object[] row = data.getValues();
			Object[] raw = data.getRawValues();

			String rawSub = raw[0].toString();
			String rawPred = raw[1].toString();

			String origSub = row[0].toString();
			String origPred = row[1].toString();
			Object origObj = row[2];
			boolean objIsString = (origObj instanceof String);
			boolean objIsUri = objIsString && raw[2].toString().startsWith("http://");

			String cleanSub = origSub;
			String cleanPred = origPred;
			Object cleanObj = origObj;

			// have to loop for all systems
			// since things like system interfaces may have more than
			// 1 system appear twice
			for(String system : systemReplacementOrder) {
				String replacementSystem = systemMapping.get(system);

				// do the replacements
				if(cleanSub.contains(system)) {
					cleanSub = cleanSub.replace(system, replacementSystem);
				}
				if(cleanPred.contains(system)) {
					cleanPred = cleanPred.replace(system, replacementSystem);
				}
				if(objIsString && cleanObj.toString().contains(system)) {
					cleanObj = cleanObj.toString().replace(system, replacementSystem);
				}
			}

			if(!cleanSub.equals(origSub) || !cleanPred.equals(origPred) || !cleanObj.equals(origObj)) {
				// need to delete this 
				// and add a new triple
				if(objIsUri) {
					removeTriples.add(new Object[] {rawSub, rawPred, raw[2].toString(), true});
				} else {
					removeTriples.add(new Object[] {rawSub, rawPred, origObj, false});
				}

				String baseSub = rawSub.substring(0, rawSub.lastIndexOf('/'));
				String basePred = rawPred.substring(0, rawPred.lastIndexOf('/'));
				if(objIsUri) {
					// URI
					String baseObj = raw[2].toString().substring(0, raw[2].toString().lastIndexOf('/'));
					addTriples.add(new Object[] {baseSub + cleanSub, basePred + cleanPred, baseObj + "/" + cleanObj, true});
				} else {
					// literal
					addTriples.add(new Object[] {baseSub + cleanSub, basePred + cleanPred, cleanObj, false});
				}
			}

			if(++counter % 10_000 == 0) {
				System.out.println("Finished " + counter + " triple checks");
			}
		}

		System.out.println("Done execution");

		System.out.println("Removing " + engine.getEngineName() + " Triples");
		System.out.println("Number of replacements = " + removeTriples.size());
	}

}
