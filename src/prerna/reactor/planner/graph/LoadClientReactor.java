package prerna.reactor.planner.graph;

import java.util.HashMap;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import prerna.engine.api.IHeadersDataRow;
import prerna.engine.api.IRawSelectWrapper;
import prerna.reactor.PixelPlanner;
import prerna.reactor.planner.AbstractLoadClient;
import prerna.sablecc2.LazyTranslation;
import prerna.sablecc2.PixelUtility;

public class LoadClientReactor extends AbstractLoadClient {

	private static final Logger LOGGER = LogManager.getLogger(LoadClientReactor.class.getName());

	protected PixelPlanner createPlanner() {
		long start = System.currentTimeMillis();

		// generate our lazy translation
		// which only ingests the routines
		// without executing

		LazyTranslation plannerT = new LazyTranslation();
		// get the iterator we are loading
		IRawSelectWrapper iterator;
		try {
			iterator = (IRawSelectWrapper) getIterator();
			String[] headers = iterator.getHeaders();
			int[] assignmentIndices = getAssignmentIndices(headers);
			int valIndex = getValueIndex(headers);
			int typeIndex = getTypeIndex(headers);
			int returnTypeIndex = getReturnTypeIndex(headers);
			String separator = getSeparator();
			if(!plannerT.getPlanner().hasProperty("MAIN_MAP", "MAIN_MAP")){
				HashMap<String, String> map = new HashMap<String, String>();
				plannerT.getPlanner().addProperty("MAIN_MAP", "MAIN_MAP", map);
			}
			HashMap<String, String> mainMap = (HashMap<String, String> )plannerT.getPlanner().getProperty("MAIN_MAP", "MAIN_MAP");
			int count = 0;
			while(iterator.hasNext()) {
				//			System.out.println(count);
				//			count++;
				IHeadersDataRow nextData = iterator.next();
				Object[] values = nextData.getValues();

				//grab the assignment variable, or the alias
				String assignment = getAssignment(values, assignmentIndices, separator);

				//grab the value we are assigning to that variable/alias
				String value = getValue(values, valIndex);	
				String returnType = getReturnType(values, returnTypeIndex);
				mainMap.put(assignment, returnType);
				//if the value is a formula add to the pksl planner
				if(isFormula(values, typeIndex)) {
					String pkslString = generatePKSLString(assignment, value);
					// skip adding self reflection pksls
					// i.e. x = (x);
					if(!AbstractPlannerReactor.isSimpleAssignment(pkslString)) {
						PixelUtility.addPixelToTranslation(plannerT, pkslString);
					}
				}
				//else we just want to add the value of the constant/decimal directly to the planner
				else{
					addVariable(plannerT.getPlanner(), assignment, value);
				}
			}
		} catch (Exception e) {
			e.printStackTrace();
		}

		// grab the planner from the new translation
		//		LOGGER.info("****************    "+total+"      *************************");
		//		LOGGER.info("****************    "+error+"      *************************");

		long end = System.currentTimeMillis();
		LOGGER.info("****************    END LOAD CLIENT "+(end - start)+"ms      *************************");

		return plannerT.getPlanner();
	}

}
