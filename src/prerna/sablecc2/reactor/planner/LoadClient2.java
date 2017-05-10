package prerna.sablecc2.reactor.planner;

import java.util.Iterator;
import java.util.List;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import prerna.engine.api.IHeadersDataRow;
import prerna.engine.api.IRawSelectWrapper;
import prerna.sablecc2.PkslUtility;
import prerna.sablecc2.PlannerTranslation;
import prerna.sablecc2.SimpleTable;
import prerna.sablecc2.om.GenRowStruct;
import prerna.sablecc2.om.Job;
import prerna.sablecc2.om.NounMetadata;
import prerna.sablecc2.om.PkslDataTypes;
import prerna.sablecc2.reactor.PKSLPlanner;
import prerna.sablecc2.reactor.TablePKSLPlanner;
import prerna.util.ArrayUtilityMethods;

public class LoadClient2 extends AbstractTablePlannerReactor {

	private static final Logger LOGGER = LogManager.getLogger(LoadClient.class.getName());

	public static final String ASSIGNMENT_NOUN = "assignment";
	public static final String VALUE_NOUN = "value";
	public static final String SEPARATOR_NOUN = "separator";

	private int total = 0;
	private int error = 0;
	
	@Override
	public void In() {
		curNoun("all");
	}

	@Override
	public Object Out() {
		return this.parentReactor;
	}

	@Override
	public NounMetadata execute()
	{
		// run through all the results in the iterator
		// and append them into a new plan
		TablePKSLPlanner newPlan = createPlanner();
		//SimpleTable.openTableServer(newPlan.getSimpleTable());
		return new NounMetadata(newPlan, PkslDataTypes.PLANNER);
	}
	
	
	private TablePKSLPlanner createPlanner() {
		long start = System.currentTimeMillis();

		// generate our lazy translation
		// which only ingests the routines
		// without executing
		PlannerTranslation plannerT = new PlannerTranslation();
		TablePKSLPlanner tplanner = new TablePKSLPlanner();
		plannerT.planner = tplanner;
		
		// get the iterator we are loading
		IRawSelectWrapper iterator = (IRawSelectWrapper) getIterator();
		String[] headers = iterator.getDisplayVariables();
		
		int[] assignmentIndices = getAssignmentIndices(headers);
		int valIndex = getValueIndex(headers);
		int typeIndex = getTypeIndex(headers);
		String separator = getSeparator();

		while(iterator.hasNext()) {
			IHeadersDataRow nextData = iterator.next();
			Object[] values = nextData.getValues();
			
			//grab the assignment variable, or the alias
			String assignment = getAssignment(values, assignmentIndices, separator);
			
			//grab the value we are assigning to that variable/alias
			String value = getValue(values, valIndex);
			
			//if the value is a formula add to the pksl planner
			if(isFormula(values, typeIndex)) {
				String pkslString = generatePKSLString(assignment, value);
				PkslUtility.addPkslToTranslation(plannerT, pkslString);
			} 
			//else we just want to add the value of the constant/decimal directly to the planner
			else {
				addVariable(plannerT.planner, assignment, value);
			}
		}
		
//		SimpleTable table = tplanner.getSimpleTable();
//		table.addcolumnIndex(table.getTableName(), );
		indexTable((TablePKSLPlanner)plannerT.planner);
		
		// grab the planner from the new translation
		LOGGER.info("****************    "+total+"      *************************");
		LOGGER.info("****************    "+error+"      *************************");

		long end = System.currentTimeMillis();
		System.out.println("****************    END LOAD CLIENT "+(end - start)+"ms      *************************");

		return (TablePKSLPlanner)plannerT.planner;
	}
	
	/**
	 * 
	 * @param planner
	 * @param assignment
	 * @param value
	 * 
	 * This method will directly add the variable to the planner instead of adding an assignment pksl
	 */
	private void addVariable(PKSLPlanner planner, String assignment, Object value) {
		NounMetadata noun = PkslUtility.getNoun(value);
		planner.addVariable(assignment, noun);
	}
	
	/**
	 * 
	 * @param assignment
	 * @param value
	 * @return
	 * 
	 * returns a pksl query given an assignment variable and the value
	 */
	private String generatePKSLString(String assignment, String value) {
		return assignment+" = "+value+";";	
	}
	
	private boolean isFormula(Object[] values, int typeIndex) {
		String value = values[typeIndex].toString();
		return "formula".equalsIgnoreCase(value);
	}

	
	/**
	 * 
	 * @param values
	 * @param assignmentIndices
	 * @param separator
	 * @return
	 */
	private String getAssignment(Object[] values, int[] assignmentIndices, String separator) {
		StringBuilder pkslBuilder = new StringBuilder();
		int numAssignments = assignmentIndices.length;
		for(int i = 0; i < assignmentIndices.length; i++) {
			// add the name
			pkslBuilder.append(values[assignmentIndices[i]]);
			if( (i+1) != numAssignments) {
				// concatenate the multiple ones with the defined value
				pkslBuilder.append(separator);
			}
		}
		
		return pkslBuilder.toString();
	}
	
	
	private String getValue(Object[] values, int valIndex) {
		return values[valIndex].toString().trim();
	}
	
	/**************************** START GET PARAMETERS *****************************************/
	/**
	 * Get the optional separator if defined
	 * @return
	 */
	private String getSeparator() {
		String separator = "";
		// this is an optional key
		// if we need to concatenate multiple things together
		if(this.store.getNounKeys().contains(SEPARATOR_NOUN)) {
			separator = this.store.getNoun(SEPARATOR_NOUN).get(0).toString();
		}
		return separator;
	}

	/**
	 * Get the job input the reactor
	 * @return
	 */
	private Iterator getIterator() {
		List<Object> jobs = this.curRow.getColumnsOfType(PkslDataTypes.JOB);
		if(jobs != null && jobs.size() > 0) {
			Job job = (Job)jobs.get(0);
			return job.getIterator();
		}

		Job job = (Job) this.store.getNoun(PkslDataTypes.JOB.toString()).get(0);
		return job.getIterator();
	}

	/**
	 * Get the unique identifier for the group of pksl scripts to load
	 * @param iteratorHeaders
	 * @return
	 */
	private int[] getAssignmentIndices(String[] iteratorHeaders) {
		// assumption that we have only one column which contains the pksl queries
		// TODO: in future, maybe allow for multiple and do a "|" between them?
		GenRowStruct assignments = this.store.getNoun(ASSIGNMENT_NOUN);
		int numAssignmentCols = assignments.size();
		int[] assignmentIndices = new int[numAssignmentCols];
		for(int index = 0; index < numAssignmentCols; index++) {
			String assignmentName = assignments.get(index).toString();
			assignmentIndices[index] = ArrayUtilityMethods.arrayContainsValueAtIndex(iteratorHeaders, assignmentName);
		}
		return assignmentIndices;
	}
	
	/**
	 * Get the index which contains the pksl scripts to load
	 * @return
	 */
	private int getValueIndex(String[] iteratorHeaders) {
		// assumption that we have only one column which contains the pksl queries
		// TODO: in future, maybe allow for multiple and do a "|" between them?
		String valueName = this.store.getNoun(VALUE_NOUN).get(0).toString();
		int valueIndex = ArrayUtilityMethods.arrayContainsValueAtIndex(iteratorHeaders, valueName);
		return valueIndex;
	}
	
	private int getTypeIndex(String[] iteratorHeaders) {
		String typeName = "Type_1";
		int typeIndex = ArrayUtilityMethods.arrayContainsValueAtIndexIgnoreCase(iteratorHeaders, typeName);
		return typeIndex;
	}
	
	/*****************END GET PARAMETERS********************/

}
