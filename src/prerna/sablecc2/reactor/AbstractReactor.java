package prerna.sablecc2.reactor;

import java.util.Arrays;
import java.util.Hashtable;
import java.util.Iterator;
import java.util.List;
import java.util.Vector;

import prerna.algorithm.api.ITableDataFrame;
import prerna.engine.api.IHeadersDataRow;
import prerna.sablecc2.om.GenRowStruct;
import prerna.sablecc2.om.NounMetadata;
import prerna.sablecc2.om.NounStore;

public abstract class AbstractReactor implements IReactor {

	protected String operationName = null;
	protected String signature = null;
	protected String curNoun = null;
	protected IReactor parentReactor = null;
	protected IReactor childReactor = null;
	protected NounStore store = null;
	protected IReactor.TYPE type = IReactor.TYPE.FLATMAP;
	protected IReactor.STATUS status = null;
	protected GenRowStruct curRow = null;
	
	protected String reactorName = "";
	protected String [] asName = null;
	protected Vector <String> outputFields = null;
	protected Vector <String> outputTypes = null;
	
	protected Hashtable <String, Object> propStore = new Hashtable<String, Object>();
	protected ITableDataFrame frame = null;
	
	protected PKSLPlanner planner = null;
	protected Lambda runner = null;
	
	protected String[] defaultOutputAlias;
	boolean evaluate = false;
	
	protected abstract void mergeUp();
	protected abstract void updatePlan();
	
	@Override
	public void setPKSL(String operation, String fullOperation) {
		this.operationName = operation;
		this.signature = fullOperation;
	}
	
	@Override
	public void setFrame(ITableDataFrame frame)
	{
		this.frame = frame;
	}

	@Override
	public String[] getPKSL() {
		String [] output = new String[2];
		output[0] = operationName;
		output[1] = signature;
		
		return output;
	}
	
	@Override
	public void setParentReactor(IReactor parentReactor) {
		this.parentReactor = parentReactor;
	}
	
	@Override
	public IReactor getParentReactor() {
		return this.parentReactor;
	}
	
	@Override
	public void setName(String reactorName) {
		this.reactorName = reactorName;
	}
	
	@Override
	public String getName() {
		return this.reactorName;
	}
	
	@Override
	public void setProp(String key, Object value) {
		propStore.put(key, value);
		
	}

	@Override
	public Object getProp(String key) {
		return propStore.get(key);
	}
	
	@Override
	public boolean hasProp(String key) {
		return propStore.containsKey(key);
	}
	
	@Override
	public void setPKSLPlanner(PKSLPlanner planner) {
		this.planner = planner;
	}
	
	@Override
	public List<NounMetadata> getOutputs() {
		return null;
	}
	
	@Override
	public void getErrorMessage() {

	}

	@Override
	public STATUS getStatus() {
		return this.status;
	}
	
	@Override
	public void setChildReactor(IReactor childReactor) {
		this.childReactor = childReactor;
	}
	
	@Override
	public void curNoun(String noun) { 
		this.curNoun = noun;
		curRow = makeNoun(noun);
	}
	
	// gets the current noun
	@Override
	public GenRowStruct getCurRow() {
		return curRow;
	}

	@Override
	public void closeNoun(String noun) {
		curRow = store.getNoun("all");
	}

	@Override
	public NounStore getNounStore() {
		// I need to see if I have a child
		// if the child
		if(this.store == null)
			store = new NounStore(operationName);
		return store;
	}

	@Override
	public TYPE getType() {
		Object typeProp = getProp("type");
		if(typeProp != null && ((String)typeProp).contains("reduce"))
			this.type = TYPE.REDUCE;
		return this.type;
	}

	@Override
	public void setAs(String [] asName) {
		this.asName = asName;
		// need to set this up on planner as well
		//if(outputFields == null)
			outputFields = new Vector();
		outputFields.addAll(Arrays.asList(asName));
		// re add this to output
		planner.addOutputs(signature, outputFields, type);
	}
	
	
	// get noun
	GenRowStruct makeNoun(String noun) {
		GenRowStruct newRow = null;
		getNounStore();
		newRow = store.makeNoun(noun);
		return newRow;
	}
	
	// execute it
	// once again this would be abstract
	public Object execute()
	{
		System.out.println("Execute the method.. " + signature);
		System.out.println("Printing NOUN Store so far.. " + store);
		return null;
	}
	
	// call for map
	public IHeadersDataRow map(IHeadersDataRow row)
	{
		return null;
	}
	
	// call for reeduce
	public Object reduce(Iterator it)
	{
		return null;
	}
	
	//provide references by which those outputs are available
	protected void addOutputLinksToPlanner() {
		if(asName == null) {
			asName = defaultOutputAlias;
		}
		
		outputFields = new Vector<String>();
		for(String alias : asName) {
			outputFields.add(alias);
		}
		
		planner.addOutputs(signature, outputFields, type);
	}
	
	protected void printReactorTrace() {
		System.out.println("THIS REACTOR: ");
		IReactor parent = this.getParentReactor();
		while(parent != null) {
			System.out.println(parent.getName());
			parent = parent.getParentReactor();
		}
		System.out.println();
	}
	
	@Override
	public List<NounMetadata> getInputs() {
		return null;
	}
	
	@Override
	public NounMetadata getOutput() {
		return null;
	}
}