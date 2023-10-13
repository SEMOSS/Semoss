package prerna.reactor;

import java.util.Hashtable;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.Stack;
import java.util.Vector;

import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.tinkergraph.structure.TinkerGraph;

import prerna.sablecc2.om.CodeBlock;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.VarStore;
import prerna.sablecc2.om.nounmeta.NounMetadata;

public class PixelPlanner {
	
	public static final String NOUN = "NOUN";
	public static final String NOUN_TYPE = "NOUN_TYPE";
	public static final String OPERATION = "OP";
	
//	public static final String REACTOR_CLASS = "REACTOR";
	public static final String TINKER_ID = "_T_ID";
	public static final String TINKER_TYPE = "_T_TYPE";
	public static final String TINKER_NAME = "_T_NAME";
	
	public static final String IN_DEGREE = "IN_DEGREE";
	public static final String PROCESSED = "PROCESSED";
	public static final String ORDER = "ORDER";
	
	// this is primarily the tinker graph that would be used for planning the operation
	public TinkerGraph g = null;
	// I need some way to have roots
	private Vector roots = new Vector();
	// and i need to keep track of the vars
	private VarStore varStore = new VarStore();
	
	public PixelPlanner() {
		this.g = TinkerGraph.open();
		addIndices();
	}
	
	public void addIndices() {
		g.createIndex(PixelPlanner.TINKER_TYPE, Vertex.class);
		g.createIndex(PixelPlanner.TINKER_ID, Vertex.class);
		g.createIndex(PixelPlanner.TINKER_ID, Edge.class);
	}
	
	public void dropIndices() {
		g.dropIndex(PixelPlanner.TINKER_TYPE, Vertex.class);
		g.dropIndex(PixelPlanner.TINKER_ID, Vertex.class);
		g.dropIndex(PixelPlanner.TINKER_ID, Edge.class);
	}
	
	public void dropGraph() {
		this.g.clear();
		this.g.close();
		this.g = null;
	}
	
	////////////////////////////////////////////////////////////////////////
	////////////////////////////////////////////////////////////////////////
	////////////////////////////////////////////////////////////////////////
	////////////////////////////////////////////////////////////////////////
	
	/*
	 * Variable methods
	 */
	
	public void addVariable(String variableName, NounMetadata value) {
		varStore.put(variableName, value);
	}
	
	public NounMetadata getVariable(String variableName) {
		return varStore.get(variableName);
	}
	
	public NounMetadata getVariableValue(String variableName) {
		return varStore.getEvaluatedValue(variableName);
	}
	
	public boolean hasVariable(String variableName) {
		return varStore.containsKey(variableName);
	}
	
	public NounMetadata removeVariable(String variableName) {
		return varStore.remove(variableName);
	}
	
	public Set<String> getVariables() {
		return varStore.getKeys();
	}
	
	public void setVarStore(VarStore varStore) {
		this.varStore = varStore;
	}
	
	public VarStore getVarStore() {
		return this.varStore;
	}
	
	////////////////////////////////////////////////////////////////////////
	////////////////////////////////////////////////////////////////////////
	////////////////////////////////////////////////////////////////////////
	////////////////////////////////////////////////////////////////////////
	
	/*
	 * New planner methods for pipeline...
	 * TODO: see how to perform previous logic (unused currently)
	 * with new structure
	 */
	
	protected Vertex upsertVertex(String type, Object name) {
		type = type.trim();
		if(name instanceof String) {
			name = name.toString().trim();
		}
		Vertex retVertex = findVertex(type, name);
		if(retVertex == null) {
			retVertex = g.addVertex(TINKER_ID, type + ":" + name, TINKER_TYPE, type, TINKER_NAME, name);
		}
		return retVertex; 
	}
	
	private Vertex findVertex(String type, Object name) {
		type = type.trim();
		if(name instanceof String) {
			name = name.toString().trim();
		}
		Vertex retVertex = null;
		GraphTraversal<Vertex, Vertex> gt = g.traversal().V().has(TINKER_ID, type + ":" + name);
		if(gt.hasNext()) {
			retVertex = gt.next();
		}
		return retVertex;
	}
	
	protected Edge upsertEdge(Vertex startV, Vertex endV, String edgeID, String type) {
		GraphTraversal<Edge, Edge> gt = g.traversal().E().has(TINKER_ID, edgeID);
		// increase the count of the in degree on the endV
		if(gt.hasNext()) {
			// this is a duplicate edge, do nothing
			return gt.next();
		} else {
			// this is a new edge
			// increase the in_degree on the end vertex
			if(!endV.property(IN_DEGREE).isPresent()) {
				endV.property(IN_DEGREE, 1);
			} else {
				int newInDegree = (int) endV.value(IN_DEGREE) + 1;
				endV.property(IN_DEGREE, newInDegree);
			}
			return startV.addEdge(edgeID, endV, TINKER_ID, edgeID, "COUNT", 1, "TYPE", type);
		}
	}
	
	public void addInputs(String opName, List<NounMetadata> inputs) {
		opName = opName.trim();
		Vertex opVertex = upsertVertex(OPERATION, opName);
		for(int inputIndex = 0; inputIndex < inputs.size(); inputIndex++) {
			NounMetadata noun = inputs.get(inputIndex);
			Object value = noun.getValue();
			PixelDataType nounType = noun.getNounType();
			if(value == null) {
				value = "null";
			}
			// if we have a lambda, then its another operation as the input
			if(nounType == PixelDataType.LAMBDA) {
				Vertex inpVertex = upsertVertex(OPERATION, value);
				String edgeID = value + "_" + opName;
				upsertEdge(inpVertex, opVertex, edgeID, "INPUT");
			} else {
				Vertex inpVertex = upsertVertex(NOUN, value);
				String edgeID = value + "_" + opName;
				upsertEdge(inpVertex, opVertex, edgeID, "INPUT");
			}
		}
	}
	
	public void addOutputs(String opName, List<NounMetadata> outputs) {
		opName = opName.trim();
		Vertex opVertex = upsertVertex(OPERATION, opName);
		for(int inputIndex = 0; inputIndex < outputs.size(); inputIndex++) {
			Object value = outputs.get(inputIndex).getValue();
			if(value == null) {
				value = "null";
			}
			Vertex inpVertex = upsertVertex(NOUN, value);
			String edgeID = value + "_" + opName;
			upsertEdge(opVertex, inpVertex, edgeID, "OUTPUT");
		}
	}
	
	
	////////////////////////////////////////////////////////////////////////
	////////////////////////////////////////////////////////////////////////
	////////////////////////////////////////////////////////////////////////
	////////////////////////////////////////////////////////////////////////

	//**********************PROPERTY METHODS*******************************//
	
	public void addProperty(String opName, String propertyName, Object value)
	{
		Vertex opVertex = upsertVertex(OPERATION, opName);
		if(opVertex != null)
		{
			opVertex.property(propertyName, value);
		}
	}
	
	public Object getProperty(String opName, String propertyName) {
		Vertex opVertex = findVertex(OPERATION, opName);
		if(opVertex != null) {
			if(opVertex.property(propertyName).isPresent()) {
				return opVertex.property(propertyName).value();
			}
		}
		return null;
	}
	
	public boolean hasProperty(String opName, String propertyName) {
		return getProperty(opName, propertyName) != null;
	}
	
	public void addNounProperty(String nounName, String propertyName, Object value)
	{
		Vertex nounVertex = findVertex(NOUN, nounName);
		if(nounVertex != null)
		{
			nounVertex.property(propertyName, value);
		}
	}

	//**********************END PROPERTY METHODS*******************************//
	
	
	//**********************IMPORTANT METHODS*******************************//

	// adds an operation with outputs
	public void addOutputs(String opName, List <String> outputs, IReactor.TYPE opType)
	{
		opName = opName.trim();
		// find the vertex for each of the input
		// wonder if this should be a full fledged block of java ?
		Vertex opVertex = upsertVertex(OPERATION, opName);
		for(int outputIndex = 0;outputIndex < outputs.size();outputIndex++)
		{
			Vertex outpVertex = upsertVertex(NOUN, outputs.get(outputIndex).trim());
			outpVertex.property("VAR_TYPE", "QUERY"); // by default it is query
			String edgeID = opName + "_" + outputs.get(outputIndex).trim();
			upsertEdge(opVertex, outpVertex, edgeID, "OUTPUT");
		}
	}

	// specify what type is the output is going to be sitting in store or is it sitting in query
	// should I also specify as names here ?
	public void addOutputs(String opName, List <String> outputs, List <String> outputTypes, IReactor.TYPE opType)
	{
		opName = opName.trim();
		// find the vertex for each of the input
		// wonder if this should be a full fledged block of java ?
		Vertex opVertex = upsertVertex(OPERATION, opName);
		for(int outputIndex = 0;outputIndex < outputs.size();outputIndex++)
		{
			Vertex outpVertex = upsertVertex(NOUN, outputs.get(outputIndex).trim());
			outpVertex.property("VAR_TYPE", outputTypes.get(outputIndex)); // by default it is query
			String edgeID = opName + "_" + outputs.get(outputIndex).trim();
			upsertEdge(opVertex, outpVertex, edgeID, "OUTPUT");
		}
	}

	public void addInputs(String opName, List <String> inputs, IReactor.TYPE opType) {
		addInputs(opName, inputs, null, opType);
	}

	// adds an operation with necessary inputs
	public void addInputs(String opName, List <String> inputs, List <String> asInputs, IReactor.TYPE opType)
	{
		// find the vertex for each of the input
		// wonder if this should be a full fledged block of java ?
		Vertex opVertex = upsertVertex(OPERATION, opName);
		opVertex.property("OP_TYPE", opType);
		opVertex.property("CODE", "NONE");
		opVertex.property("INPUT_ORDER", inputs);
		Vector <String> aliasVector = new Vector<String>();
		for(int inputIndex = 0;inputIndex < inputs.size();inputIndex++)
		{
			Vertex inpVertex = upsertVertex(NOUN, inputs.get(inputIndex).trim());
			if(asInputs != null && inputIndex < asInputs.size()) {
				aliasVector.add(asInputs.get(inputIndex));
			} else {
				aliasVector.add(inputs.get(inputIndex).trim());
			} 
			String edgeID = inputs.get(inputIndex).trim() + "_" + opName;
			// make sure we do not add the edges multiple times
			upsertEdge(inpVertex, opVertex, edgeID, "INPUT");
		}
		opVertex.property("ALIAS", aliasVector);
	}

	//**********************END IMPORTANT METHODS*******************************//

	public void addInputs(String opName, CodeBlock codeBlock, List <String> inputs, IReactor.TYPE opType)
	{
		addInputs(opName, codeBlock, inputs, null, opType);
	}
	
	// adds a java operation string as input
	// this should be more of outputs and not inputs
	// face palm
	// in this case the as Name should be used as the main name
	// I see seems like I have accomodated for multiple as names
	// no not really it is just as here
	public void addInputs(String opName, CodeBlock codeBlock, List <String> inputs, List <String> asInputs, IReactor.TYPE opType)
	{
		// should this be string ?
		// it should be a map block
		// or it should be a reduce block
		// at a minimum it should say what type of block it is
		opName = opName.trim();
		Vertex opVertex = upsertVertex(OPERATION, opName);
		opVertex.property("OP_TYPE", opType);
		opVertex.property("CODE", codeBlock);
		opVertex.property("INPUT_ORDER", inputs);
		List <String> aliasVector = new Vector<String>();
		for(int inputIndex = 0;inputIndex < inputs.size();inputIndex++)
		{
			Vertex inpVertex = upsertVertex(NOUN, inputs.get(inputIndex).trim());
			if(asInputs != null && inputIndex < asInputs.size()) {
				aliasVector.add(asInputs.get(inputIndex).trim());
			} else {
				aliasVector.add(inputs.get(inputIndex).trim());
			}
			String edgeID = inputs.get(inputIndex).trim()+ "_" + opName;
			upsertEdge(inpVertex, opVertex, edgeID, "INPUT");
		}
		opVertex.property("ALIAS", aliasVector);
	}

	// this runs the plan for the final output
	public Iterator runMyPlan(String output)
	{
		// I need to take all of the stuff from gremlin and assort it into
		// blocks
		// Blocks can be progressively executed
		// I need to stack the blocks
		// everytime I find the input to be a block, I need to stack it up i.e. only the code operation
		// Output <- codeBlock <- codeblock etc. 
		// Everytime I see a reduce operation I need to chop it off
		// And So on
		
		// while output input edges is all not null
		// I need to always assimilate all the inputs
		// and plan my operation
		// I need a block keeper 
		
		// when there is a reduce which multiple thing depends on, I need to validatee so that it doesn't do it twice
		// Just as a stage has various operations and their sequence
		// I also need to keep something with all stages and its sequence
		// but at the same time ensure that it is not overwriting anything
		
		
		
		// I need to test
		// Single Stage - done
		// Multi Stage with reduce - done
		// Multi stage with reduce 
		
		List <Object> inputs = new Vector<Object>();
		Vertex workingVertex = findVertex(OPERATION, output);
		StageKeeper allStages = new StageKeeper();
		if(workingVertex != null)
		{
			Stack <Hashtable<String, Object>> codeStack = new Stack<Hashtable<String, Object>>();
			
			// INPUT inputVector, CODE codeVector, OUTPUT outputVector
			Hashtable <String, Object> curHash = new Hashtable<String, Object>();
			List <Vertex> verticesToProcess = new Vector<Vertex>();
			verticesToProcess.add(workingVertex);
			curHash.put("VERTEX_TO_PROCESS", verticesToProcess);
			codeStack.push(curHash);
			
			String code = null;
			//code = loadVertex(codeStack);
			
			//Stack <Stage> newStack = new Stack<Stage>();
			//code = loadVertex2(newStack, verticesToProcess);
			
			allStages = loadVertex3(allStages, verticesToProcess);
			// execute the code and return the iterator
		}	
		
		// run all the stages
		// need to do a couple of things
		// get the stage
		// if there are no more stages, then return the iterator here
		return allStages.processStages();
	}
	
	

	private StageKeeper loadVertex3(StageKeeper keeper, List <Vertex> verticesToProcess)
	{
		
		// I need to break this into stages i.e. the codeStack
		Stage thisStage = null;
		if(keeper.lastStage != null)
			thisStage = keeper.lastStage;
		else
		{
			thisStage = new Stage();
			thisStage.stageNum = 0;
		}
		Vertex thisVertex = verticesToProcess.remove(0);
		System.out.println("Processing Vertex >>>>>>>>>>>>>>>>>>>> " + thisVertex.property(TINKER_NAME));

		String operationName = thisVertex.property(TINKER_NAME).value() + "";
		keeper.addStage(operationName, thisStage);
		
		Hashtable opHash = thisStage.addOperation(operationName);

		Hashtable <String, String> inputTypes = new Hashtable<String, String>();
		Vector <String> opInputs = (Vector<String>)opHash.get(Stage.INPUTS);
		Vector <String> deInputs = (Vector<String>)opHash.get(Stage.DERIVED_INPUTS);
		Vector <String> depends = (Vector<String>)opHash.get(Stage.DEPENDS);
		// set the name as the signature
		opHash.put("SIGNATURE", operationName);
		if(thisVertex.property("INPUT_ORDER").isPresent())
			opHash.put("INPUT_ORDER", thisVertex.property("INPUT_ORDER").value());
		if(thisVertex.property("FILTERS").isPresent())
			opHash.put("FILTERS", thisVertex.property("FILTERS").value());
		if(thisVertex.property("JOINS").isPresent())
			opHash.put("JOINS", thisVertex.property("JOINS").value());
		// takes the frame if it is available
		if(thisVertex.property("FRAME").isPresent())
			opHash.put("FRAME", thisVertex.property("FRAME").value());
		// while we are at it might as well synchronize the property store
		if(thisVertex.property("STORE").isPresent())
			opHash.put("STORE", thisVertex.property("STORE").value());
		if(thisVertex.property("ALIAS").isPresent())
			opHash.put("ALIAS", thisVertex.property("ALIAS").value());
		opHash.put("OP_TYPE", thisVertex.property("OP_TYPE").value());
		// add reactor to the stage should 1 be present
		if(thisVertex.property("REACTOR").isPresent())
			thisStage.stageStore.put("REACTOR", thisVertex.property("REACTOR").value());

		//String [] curCode = (String [])opHash.get(Stage.CODE);
		// we will refine this code piece later

		// run through each input 
		// and 
		// the input to these are almost.. wait always nouns
		// I need a nounstore to get this assigned
		// this way I can keep the filters and everything else
		// should I just load the reactor ?
		// that way I can get what I want ?
		// the problem then is I have to worry about filters
		// let the filters be for now.. let us run through other things
		boolean close = false;

		// assimilate the inputs in a single place
		IReactor.TYPE opType = null;
		Object codeBlock = null;
		try {
			codeBlock = thisVertex.property("CODE").value();
			opType = (IReactor.TYPE)thisVertex.property("OP_TYPE").value();
		} catch(Exception e) {
			
		}
		
		// add the code
//		Object codeBlock = thisVertex.property("CODE").value();
		
		// write the code if you see a piece of code
		if(codeBlock instanceof CodeBlock)
		{
			// I need to print this code
			String [] code = ((CodeBlock)codeBlock).getCode();
			//curCode = curCode + "\n" + code; // building from the bottom
			opHash.put(Stage.CODE, code);
		}
		// add inputs to these and in the end synchronize
		// this might get the as so something to be aware of for now
		Iterator <Vertex> inputs = thisVertex.vertices(Direction.IN);

		// find if this is a reduce operation
		while(inputs.hasNext())
		{
			// I need to account for when the input is an operation
			// vs. when the input is not an operation
			
			// collect the operations that we need to process next
			// as well as columns it is looking for
			// get this vertex
			// not accounting for parallel run yet, I will get to it
			Vertex thisNoun = inputs.next();
			String nounName = thisNoun.property(TINKER_NAME).value() + "";
			
			System.out.println("Adding noun.. " + nounName);
			
			if(thisNoun.property("VAR_TYPE").isPresent())
				inputTypes.put(nounName, thisNoun.property("VAR_TYPE").value() + "");
			
			// add if this column is not there already
			// I should do this at a later point because I dont know when it will be a reduce..
			//if(allInputCols.indexOf(nounName) < 0)
			//	allInputCols.add(thisNoun.property(TINKER_NAME).value() + "");
			
			// get the input for this to find if this is a reduce
			// this will be a an operation here
			// what is producing this noun
			Iterator <Vertex> opVertices = thisNoun.vertices(Direction.IN);
			
			Vertex thisOp = null;
			if(opVertices.hasNext()) // ok there is an operation
			{
				thisOp = opVertices.next();
				
				// also need to add the dependency
				
				// add this for the next run
				// take it out and add it to the end so it comes towards the end
				System.out.println("\t Vertex" + thisOp.property(TINKER_NAME).value());
				if(verticesToProcess.indexOf(thisOp) >= 0)
					verticesToProcess.remove(thisOp);
				verticesToProcess.add(thisOp);		
				depends.addElement(thisOp.property(TINKER_NAME).value() + "");
				// I need to find if this is reduce and if do find what do I do again ?
				// some how I need to move to next code block
				// no idea what this means hehe.. 
				// need to close this block after this is done
				codeBlock = thisOp.property("CODE").value();
				if(codeBlock instanceof String)
					opInputs.add(nounName);
				else // this is a derived call 
					deInputs.add(nounName);
			}
			else // this is the base input with no parent operation
			{
				if(opInputs.indexOf(nounName) < 0)
					opInputs.add(nounName);
			}
		}
		

		
		// I need to add the decorator on top
		// so that this will only be those fields that this particular routine needs
		// so I could be retrieving [a,b,c,d,e] and this routine may only require
		// [a,d,e] so I need to seggregate before passing it on
		// this is all sit on the input Cols
		opHash.put(Stage.INPUT_TYPE, inputTypes);
		opHash.put(Stage.INPUTS, opInputs);
		opHash.put(Stage.DERIVED_INPUTS, deInputs);
		//opHash.put(Stage.CODE, curCode);	
		opHash.put(Stage.DEPENDS, depends);
		
		if(IReactor.TYPE.REDUCE.equals(opType)) // this is a reduce operation
		{
			// couple of things I need to do
			// I need to add the query to the top of it
			// which means I need sum total of all columns
			// need to take all the input columns and generate the query for it
			// this is where I convert this into a query and then let it rip from here on
			// close out the first piece
			
			// remove this operation
			// add a new stage
			// add this operation to the new stage
			// recurse
			thisStage.removeOperation(operationName);
			// remove the reactor too
			thisStage.stageStore.remove(operationName);
			Stage newStage = new Stage();
			// add the reactor if the user has passed that in
			if(thisVertex.property("REACTOR").isPresent())
				newStage.stageStore.put("REACTOR", thisVertex.property("REACTOR").value());
			
			int newNum = thisStage.stageNum + 1;
			newStage.stageNum = newNum;
			newStage.addOperation(operationName, opHash);			
			keeper.addStage(operationName, newStage);
			//codeStack.push(newStage);
			thisStage = newStage;
		}
		//codeStack.push(thisStage);
		thisStage.synchronizeInput(operationName);
		
		if(verticesToProcess.size() > 0)
		{
			return loadVertex3(keeper, verticesToProcess);
		}

		keeper.adjustStages();
		keeper.printCode();
		// walk through the code stack poping one by one
		// and asking it to getcode
		
		System.out.println("Final CODE..\n\n\n\n");
		//System.out.println(curCode);	
		return keeper;
	}

	
	private String loadVertex2(Stack <Stage> codeStack, Vector <Vertex> verticesToProcess)
	{
		
		// I need to break this into stages i.e. the codeStack
		Stage thisStage = null;
		if(codeStack.size() > 0)
			thisStage = codeStack.pop();
		else
		{
			thisStage = new Stage();
			thisStage.stageNum = 0;
		}
		Vertex thisVertex = verticesToProcess.remove(0);
		System.out.println("Processing Vertex >>>>>>>>>>>>>>>>>>>> " + thisVertex.property(TINKER_NAME));

		String operationName = thisVertex.property(TINKER_NAME).value() + "";
		
		Hashtable opHash = thisStage.addOperation(operationName);
		// add inputs to these and in the end synchronize
		// this might get the as so something to be aware of for now
		Iterator <Vertex> inputs = thisVertex.vertices(Direction.IN);

		Vector <String> opInputs = (Vector<String>)opHash.get(Stage.INPUTS);
		//Vector <String> asInputs = (Vector<String>)opHash.get(Stage.AS_INPUTS);
		Vector <String> deInputs = (Vector<String>)opHash.get(Stage.DERIVED_INPUTS);
		Vector <String> depends = (Vector<String>)opHash.get(Stage.DEPENDS);
		String curCode = (String)opHash.get(Stage.CODE);
		// we will refine this code piece later

		// run through each input 
		// and 
		// the input to these are almost.. wait always nouns
		// I need a nounstore to get this assigned
		// this way I can keep the filters and everything else
		// should I just load the reactor ?
		// that way I can get what I want ?
		// the problem then is I have to worry about filters
		// let the filters be for now.. let us run through other things
		boolean close = false;

		// assimilate the inputs in a single place
		IReactor.TYPE opType = (IReactor.TYPE)thisVertex.property("OP_TYPE").value();
		
		// add the code
		Object codeBlock = thisVertex.property("CODE").value();
		
		// write the code if you see a piece of code
		if(codeBlock instanceof CodeBlock)
		{
			// I need to print this code
			String [] code = ((CodeBlock)codeBlock).getCode();
			curCode = curCode + "\n" + code; // building from the bottom
			opHash.put(Stage.CODE, curCode);
		}

		// find if this is a reduce operation
		while(inputs.hasNext())
		{
			// I need to account for when the input is an operation
			// vs. when the input is not an operation
			
			// collect the operations that we need to process next
			// as well as columns it is looking for
			// get this vertex
			// not accounting for parallel run yet, I will get to it
			Vertex thisNoun = inputs.next();
			String nounName = thisNoun.property(TINKER_NAME).value() + "";
			
			System.out.println("Adding noun.. " + nounName);
			
			// add if this column is not there already
			// I should do this at a later point because I dont know when it will be a reduce..
			//if(allInputCols.indexOf(nounName) < 0)
			//	allInputCols.add(thisNoun.property(TINKER_NAME).value() + "");
			
			// get the input for this to find if this is a reduce
			// this will be a an operation here
			// what is producing this noun
			Iterator <Vertex> opVertices = thisNoun.vertices(Direction.IN);
			
			Vertex thisOp = null;
			if(opVertices.hasNext()) // ok there is an operation
			{
				thisOp = opVertices.next();
				
				// also need to add the dependency
				
				// add this for the next run
				// take it out and add it to the end so it comes towards the end
				System.out.println("\t Vertex" + thisOp.property(TINKER_NAME).value());
				if(verticesToProcess.indexOf(thisOp) >= 0)
					verticesToProcess.remove(thisOp);
				verticesToProcess.add(thisOp);		
				depends.addElement(thisOp.property(TINKER_NAME).value() + "");
				// I need to find if this is reduce and if do find what do I do again ?
				// some how I need to move to next code block
				// no idea what this means hehe.. 
				// need to close this block after this is done
				codeBlock = thisOp.property("CODE").value();
				if(codeBlock instanceof String)
					opInputs.add(nounName);
				else // this is a derived call 
					deInputs.add(nounName);
			}
			else // this is the base input with no parent operation
				opInputs.add(nounName);
		}
		

		
		// I need to add the decorator on top
		// so that this will only be those fields that this particular routine needs
		// so I could be retrieving [a,b,c,d,e] and this routine may only require
		// [a,d,e] so I need to seggregate before passing it on
		// this is all sit on the input Cols
		opHash.put(Stage.INPUTS, opInputs);
		opHash.put(Stage.DERIVED_INPUTS, deInputs);
		opHash.put(Stage.CODE, curCode);	
		opHash.put(Stage.DEPENDS, depends);
		
		if(opType == IReactor.TYPE.REDUCE) // this is a reduce operation
		{
			// couple of things I need to do
			// I need to add the query to the top of it
			// which means I need sum total of all columns
			// need to take all the input columns and generate the query for it
			// this is where I convert this into a query and then let it rip from here on
			// close out the first piece
			
			// remove this operation
			// add a new stage
			// add this operation to the new stage
			// recurse
			thisStage.removeOperation(operationName);
			Stage newStage = new Stage();
			codeStack.push(thisStage);
			int newNum = thisStage.stageNum + 1;
			newStage.stageNum = newNum;
			newStage.addOperation(operationName, opHash);			
			//codeStack.push(newStage);
			thisStage = newStage;
		}
		codeStack.push(thisStage);
		thisStage.synchronizeInput(operationName);
		
		if(verticesToProcess.size() > 0)
		{
			return loadVertex2(codeStack, verticesToProcess);
		}
		
		// walk through the code stack poping one by one
		// and asking it to getcode
		for(int stageIndex = codeStack.size()-1;stageIndex >= 0;stageIndex--)
		{
			System.out.println("====================================");
			Stage stage = codeStack.elementAt(stageIndex);
			System.out.println(" STAGE " + (codeStack.size() - stageIndex) + " \n\n" + stage.getCode());
			System.out.println("====================================");
		}
		
		System.out.println("Final CODE..\n\n\n\n");
		System.out.println(curCode);	
		return curCode;
	}

	
	// I need to keep track of
	// the code so far
	// inputs so far for this code block so I can have one query
	// that is it
	// I need a master code structure
	// which says what stage this particular processing
	// each stage should have an operation
	// query that defines this stage
	// and code block for every stage - this could quite simply be just a string
	// stage is composed each of the operation and the respective code
	// whenever there is an existing piece of code it should take the order and reinsert it to top i.e. vector
	// I also need 
	private String loadVertex(Stack <Hashtable<String, Object>> codeStack)
	{
		
		// I need to break this into stages i.e. the codeStack
		Hashtable <String, Object> curHash = codeStack.pop();
		Vector <Vertex> verticesToProcess = (Vector<Vertex>)curHash.get("VERTEX_TO_PROCESS");
		Vertex thisVertex = verticesToProcess.remove(0);
		System.out.println("Processing Vertex >>>>>>>>>>>>>>>>>>>> " + thisVertex.property(TINKER_NAME));
		// this might get the as so something to be aware of for now
		Iterator <Vertex> inputs = thisVertex.vertices(Direction.IN);
		Vector <String> allInputCols = new Vector<String>();

		String curCode = "}";
		// we will refine this code piece later
		if(curHash.containsKey("CODE"))
			curCode = (String)curHash.get("CODE");

		if(curHash.containsKey("ALL_INPUTS"))
			allInputCols = (Vector<String>)curHash.get("ALL_INPUTS");

		// run through each input 
		// and 
		// the input to these are almost.. wait always nouns
		// I need a nounstore to get this assigned
		// this way I can keep the filters and everything else
		// should I just load the reactor ?
		// that way I can get what I want ?
		// the problem then is I have to worry about filters
		// let the filters be for now.. let us run through other things
		boolean close = false;
		// assimilate the inputs in a single place
		
		Vector <String> inputCols = new Vector <String>();
		Vector <String> derivedCols = new Vector<String>();
		
		IReactor.TYPE opType = (IReactor.TYPE)thisVertex.property("OP_TYPE").value();
		
		
		// add the code
		Object codeBlock = thisVertex.property("CODE").value();
		
		// write the code if you see a piece of code
		if(codeBlock instanceof CodeBlock)
		{
			// I need to print this code
			String [] code = ((CodeBlock)codeBlock).getCode();
			curCode = code + curCode; // building from the bottom
		}

		String nounSoFar = "";
		
		// find if this is a reduce operation
		while(inputs.hasNext())
		{
			// I need to account for when the input is an operation
			// vs. when the input is not an operation
			
			// collect the operations that we need to process next
			// as well as columns it is looking for
			// get this vertex
			// not accounting for parallel run yet, I will get to it
			Vertex thisNoun = inputs.next();
			String nounName = thisNoun.property(TINKER_NAME).value() + ""; 
			System.out.println("Adding noun.. " + nounName);
			nounSoFar = nounSoFar + "\t" + nounName;
			
			// add if this column is not there already
			// I should do this at a later point because I dont know when it will be a reduce..
			//if(allInputCols.indexOf(nounName) < 0)
			//	allInputCols.add(thisNoun.property(TINKER_NAME).value() + "");
			
			// get the input for this to find if this is a reduce
			// this will be a an operation here
			// what is producing this noun
			Iterator <Vertex> opVertices = thisNoun.vertices(Direction.IN);
			
			Vertex thisOp = null;
			if(opVertices.hasNext()) // ok there is an operation
			{
				thisOp = opVertices.next();
				// add this for the next run
				// take it out and add it to the end so it comes towards the end
				System.out.println("\t Vertex" + thisOp.property(TINKER_NAME).value());
				if(verticesToProcess.indexOf(thisOp) >= 0)
					verticesToProcess.remove(thisOp);
				verticesToProcess.add(thisOp);					
				// I need to find if this is reduce and if do find what do I do again ?
				// some how I need to move to next code block
				// no idea what this means hehe.. 
				// need to close this block after this is done
				codeBlock = thisOp.property("CODE").value();
				if(codeBlock instanceof String)
					inputCols.add(nounName);
				else // this is a derived call 
					derivedCols.add(nounName);
			}
			else // this is the base input with no parent operation
				inputCols.add(nounName);
				
		}
		
		// I need to add the decorator on top
		// so that this will only be those fields that this particular routine needs
		// so I could be retrieving [a,b,c,d,e] and this routine may only require
		// [a,d,e] so I need to seggregate before passing it on
		// this is all sit on the input Cols
		if(opType != IReactor.TYPE.REDUCE)
		{
			// add the inputcols to the all input cols
			for(int inputIndex = 0;inputIndex <inputCols.size();inputIndex++)
				if(allInputCols.indexOf(inputCols.elementAt(inputIndex)) < 0 ) allInputCols.add(inputCols.elementAt(inputIndex));
			
			curHash.put("ALL_INPUTS", allInputCols);

			// add the curVerticesToProcess to all vertices.
			// You are good to go proceed.
			String columnFilter = "getColumns(inputCols);\n"; // this is pseudo code right now
			String mapOp = "OPERATION NAME : " + thisVertex.property(TINKER_NAME).value() + " \n COLUMN SUBSET..  " + nounSoFar + "\n";
			curCode = columnFilter + mapOp + curCode;
		}
		else // this is a reduce operation
		{
			// couple of things I need to do
			// I need to add the query to the top of it
			// which means I need sum total of all columns
			// need to take all the input columns and generate the query for it
			// this is where I convert this into a query and then let it rip from here on
			// close out the first piece
			String query = "String query = composeQuery(allInputCols);\n";
			String columns = "QUERY COLUMNS NEEDED : ";
			for(int inputIndex = 0;inputIndex < allInputCols.size();inputIndex++)
				columns = columns + " \t " + allInputCols.elementAt(inputIndex);
			String execute = "execute this query ";
			String foreach = "while(result.hasNext()) {";
			String mapLogic = "// here comes map logic :)";
			curCode = query + execute + mapLogic + foreach + curCode;
			
			// need to put this portion of the code too
			// but this is reduce
			// doesn't matter
			String columnFilter = "getColumns(inputCols);\n"; // this is pseudo code right now
			curCode = columnFilter + curCode;
			
			query = "String query = composeQuery(inputCols);";
			columns = "QUERY COLUMN NEEDED : " + nounSoFar + "\n";
			execute = "execute this query";
			String reduceLogic = "// here comes the reduce logic :) " + thisVertex.property(TINKER_NAME).value() ;
			foreach = "while(result.hasNext()) {";
			curCode = query + columns + execute + reduceLogic + foreach + "}" + curCode;

			// create a new Hash for the next cycle
			curHash = new Hashtable <String, Object>();				
		}
		if(verticesToProcess.size() > 0)
		{
			curHash.put("VERTEX_TO_PROCESS", verticesToProcess);
			curHash.put("CODE", curCode);
			curHash.put("ALL_INPUTS", allInputCols);
			codeStack.push(curHash);
			return loadVertex(codeStack);
		}
		else
		{
			// close out
			String query = "String query = composeQuery(allInputCols);\n";
			String columns = "QUERY COLUMNS NEEDED FINAL: ";
			for(int inputIndex = 0;inputIndex < allInputCols.size();inputIndex++)
				columns = columns + " \t " + allInputCols.elementAt(inputIndex);
			String execute = "\n execute this query \n";
			String foreach = "while(result.hasNext()) { \n";
			String mapLogic = "// here comes map logic :) " + thisVertex.property(TINKER_NAME).value() + "\n";
			curCode = query + columns + execute + mapLogic + foreach + curCode;			
		}
		System.out.println("Final CODE..\n\n\n\n");
		System.out.println(curCode);	
		return curCode;
	}
}
