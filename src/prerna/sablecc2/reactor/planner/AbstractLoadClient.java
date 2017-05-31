package prerna.sablecc2.reactor.planner;

import java.util.Iterator;
import java.util.List;

import prerna.sablecc2.PkslUtility;
import prerna.sablecc2.om.GenRowStruct;
import prerna.sablecc2.om.Job;
import prerna.sablecc2.om.NounMetadata;
import prerna.sablecc2.om.PkslDataTypes;
import prerna.sablecc2.reactor.AbstractReactor;
import prerna.sablecc2.reactor.PKSLPlanner;
import prerna.util.ArrayUtilityMethods;

public abstract class AbstractLoadClient extends AbstractReactor {

	public static final String ASSIGNMENT_NOUN = "assignment";
	public static final String VALUE_NOUN = "value";
	public static final String SEPARATOR_NOUN = "separator";
	public static final String TYPE_NOUN = "type";
	
	/**
	 * 
	 * @return
	 * 
	 * This abtract method is defined by the child class to have control as to what type of planner to create, all other operations are the same
	 */
	protected abstract PKSLPlanner createPlanner();
	
	@Override
	public NounMetadata execute()
	{
		// run through all the results in the iterator
		// and append them into a new plan
		PKSLPlanner newPlan = createPlanner();
		return new NounMetadata(newPlan, PkslDataTypes.PLANNER);
	}
	
	/**
	 * 
	 * @param planner
	 * @param assignment
	 * @param value
	 * 
	 * This method will directly add the variable to the planner instead of adding an assignment pksl
	 */
	protected void addVariable(PKSLPlanner planner, String assignment, Object value) {
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
	protected String generatePKSLString(String assignment, String value) {
		return assignment+" = "+value+";";	
	}
	
	protected boolean isFormula(Object[] values, int typeIndex) {
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
	protected String getAssignment(Object[] values, int[] assignmentIndices, String separator) {
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
	
	
	protected String getValue(Object[] values, int valIndex) {
		return values[valIndex].toString().trim();
	}
	
	/**************************** START GET PARAMETERS *****************************************/
	/**
	 * Get the optional separator if defined
	 * @return
	 */
	protected String getSeparator() {
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
	protected Iterator getIterator() {
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
	protected int[] getAssignmentIndices(String[] iteratorHeaders) {
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
	protected int getValueIndex(String[] iteratorHeaders) {
		// assumption that we have only one column which contains the pksl queries
		// TODO: in future, maybe allow for multiple and do a "|" between them?
		String valueName = this.store.getNoun(VALUE_NOUN).get(0).toString();
		int valueIndex = ArrayUtilityMethods.arrayContainsValueAtIndex(iteratorHeaders, valueName);
		return valueIndex;
	}
	
	protected int getTypeIndex(String[] iteratorHeaders) {
		String typeName;
		if(this.store.getNounKeys().contains(TYPE_NOUN)) {
			typeName = this.store.getNoun(TYPE_NOUN).get(0).toString();
		} else {
			typeName = "Type_1";
		}
		int typeIndex = ArrayUtilityMethods.arrayContainsValueAtIndexIgnoreCase(iteratorHeaders, typeName);
		return typeIndex;
	}
}
