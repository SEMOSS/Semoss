package prerna.sablecc2.reactor.storage;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PushbackReader;
import java.util.Iterator;
import java.util.List;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Vertex;

import prerna.engine.api.IHeadersDataRow;
import prerna.engine.api.IRawSelectWrapper;
import prerna.sablecc2.PlannerTranslation;
import prerna.sablecc2.lexer.Lexer;
import prerna.sablecc2.lexer.LexerException;
import prerna.sablecc2.node.Start;
import prerna.sablecc2.om.GenRowStruct;
import prerna.sablecc2.om.Job;
import prerna.sablecc2.om.NounMetadata;
import prerna.sablecc2.om.PkslDataTypes;
import prerna.sablecc2.parser.Parser;
import prerna.sablecc2.parser.ParserException;
import prerna.sablecc2.reactor.AbstractReactor;
import prerna.sablecc2.reactor.PKSLPlanner;
import prerna.util.ArrayUtilityMethods;

public class LoadClient extends AbstractReactor {

	private static final Logger LOGGER = LogManager.getLogger(LoadClient.class.getName());

	public static final String ASSIGNMENT_NOUN = "assignment";
	public static final String VALUE_NOUN = "value";
	public static final String SEPARATOR_NOUN = "separator";

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
		PKSLPlanner newPlan = createPlanner();

		// for debugging
		// right now just print the plan
		printPlan(newPlan);


		//replace this planner with generated planner
		this.planner = newPlan;

		return null;
	}
	
	private PKSLPlanner createPlanner() {
		// generate our lazy translation
		// which only ingests the routines
		// without executing
		PlannerTranslation plannerT = new PlannerTranslation();
		
		// get the iterator we are loading
		IRawSelectWrapper iterator = (IRawSelectWrapper)getIterator();
		String[] headers = iterator.getDisplayVariables();
		
		int[] assignmentIndices = getAssignmentIndices(headers);
		int valIndex = getValueIndex(headers);
		String separator = getSeparator();
		
		// as we iterate through
		// run the values through the planner
		while(iterator.hasNext()) {
			IHeadersDataRow nextData = iterator.next();
			String pkslString = generatePKSLString(nextData.getValues(), assignmentIndices, valIndex, separator);
			
			if(pkslString.contains("IF")) {
				System.out.println("");
			}
			
			try {
				Parser p = new Parser(new Lexer(new PushbackReader(new InputStreamReader(new ByteArrayInputStream(pkslString.getBytes("UTF-8"))))));
				Start tree = p.parse();
				tree.apply(plannerT);
			} catch (ParserException | LexerException | IOException e) {
				e.printStackTrace();
			}
		}
		
		// grab the planner from the new translation
		return plannerT.planner;
	}
	
	private String generatePKSLString(Object[] values, int[] assignmentIndices, int valIndex, String separator) {
		StringBuilder pkslBuilder = new StringBuilder();
		pkslBuilder.append("\"");
		
		// add the assignment
		int numAssignments = assignmentIndices.length;
		for(int i = 0; i < numAssignments; i++) {
			// add the name
			pkslBuilder.append(values[assignmentIndices[i]]);
			if( (i+1) != numAssignments) {
				// concatenate the multiple ones with the defined value
				pkslBuilder.append(separator);
			}
		}
		// add the equals and value
		pkslBuilder.append("\"=").append(values[valIndex]).append(";");
		
		// return the new pksl
		return pkslBuilder.toString();
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
	
	/**
	 * Get the optional separator if defined
	 * @return
	 */
	private String getSeparator() {
		String separator = null;
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

		Job job = (Job)this.getNounStore().getNoun(PkslDataTypes.JOB.toString()).get(0);
		return job.getIterator();
	}

	//method to see the structure of the plan
	private void printPlan(PKSLPlanner planner) {
		System.out.println(">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>");
		System.out.println(">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>");
		System.out.println(">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>");
		System.out.println(">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>");
		System.out.println(">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>");
		System.out.println(">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>");
		System.out.println(">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>");
		GraphTraversal<Vertex, Vertex> getAllV = planner.g.traversal().V().has(PKSLPlanner.TINKER_TYPE, PKSLPlanner.OPERATION);
		while(getAllV.hasNext()) {
			// get the vertex
			Vertex v = getAllV.next();
			System.out.println(">> " + v.property(PKSLPlanner.TINKER_ID).value().toString());

			// all of the vertex inputs
			Iterator<Edge> inputEdges = v.edges(Direction.IN);
			while(inputEdges.hasNext()) {
				Edge inputE = inputEdges.next();
				Vertex inputV = inputE.outVertex();
				System.out.println("\tinput >> " + inputV.property(PKSLPlanner.TINKER_ID).value().toString());
			}

			// all of the vertex inputs
			Iterator<Edge> outputEdges = v.edges(Direction.OUT);
			while(outputEdges.hasNext()) {
				Edge outputE = outputEdges.next();
				Vertex outputV = outputE.inVertex();
				System.out.println("\toutput >> " + outputV.property(PKSLPlanner.TINKER_ID).value().toString());
			}
		}
	}


}
