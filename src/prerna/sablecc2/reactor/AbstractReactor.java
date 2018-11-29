package prerna.sablecc2.reactor;

import java.math.BigDecimal;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Hashtable;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Vector;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.codehaus.plexus.util.StringUtils;

import prerna.engine.api.IHeadersDataRow;
import prerna.om.Insight;
import prerna.sablecc2.comm.InMemoryConsole;
import prerna.sablecc2.om.GenRowStruct;
import prerna.sablecc2.om.NounStore;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.execptions.SemossPixelException;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.sablecc2.reactor.job.JobReactor;

public abstract class AbstractReactor implements IReactor {

	private static final Logger LOGGER = LogManager.getLogger(AbstractReactor.class.getName());

	protected Insight insight = null;
	public PixelPlanner planner = null;
	public Vector<IReactor> childReactor = new Vector<IReactor>();

	protected String operationName = null;
	protected String signature = null;
	protected String originalSignature = null;

	protected String curNoun = null;
	protected IReactor parentReactor = null;
	
	protected NounStore store = null;
	protected IReactor.TYPE type = IReactor.TYPE.FLATMAP;
	protected IReactor.STATUS status = null;
	protected GenRowStruct curRow = null;
	
	protected String reactorName = "";
	protected String [] asName = null;
	protected Vector <String> outputFields = null;
	protected Vector <String> outputTypes = null;
	
	protected Hashtable <String, Object> propStore = new Hashtable<String, Object>();
	
	protected Lambda runner = null;
	
	protected String[] defaultOutputAlias;
	boolean evaluate = false;
	
	protected String jobId;
	
	public String[] keysToGet = new String[]{"no keys defined"};
	public Map<String, String> keyValue = new Hashtable<String, String>();

	// convenience method to allow order or named noun
	public void organizeKeys()
	{
		if(this.getNounStore().size() > 1) {
			for(int keyIndex = 0; keyIndex < keysToGet.length; keyIndex++) {
				String key = keysToGet[keyIndex];
				if(this.store.getNoun(key) != null) {
					GenRowStruct grs = this.store.getNoun(key);
					if(!grs.isEmpty()) {
						keyValue.put(keysToGet[keyIndex], grs.get(0)+"");	
					}
				}
			}
		}
		
		// if we still are empty
		// try to fill via input indices in cur row
		if(keyValue.isEmpty()) {
			GenRowStruct struct = this.getCurRow();
			int structSize = struct.size();
			for(int keyIndex = 0; keyIndex < keysToGet.length && keyIndex < structSize; keyIndex++) {
				keyValue.put(keysToGet[keyIndex], struct.get(keyIndex)+"");
			}
		}
	}
	
	/**
	 * 	Organize a list of engines into the keyValue object of the reactor for
	 *  engine name related searches in solr.
	 * @param enginesList List of Engines(name) for the specific user
	 */
	public void organizeKeysForEngines(HashSet<String> enginesList) {
		int enginesSize = enginesList.size();
		String[] engines = enginesList.toArray(new String[enginesList.size()]);
		for (int enIdx = 0; enIdx < enginesSize; enIdx++) {
			if (enIdx == 0)
				keyValue.put(this.keysToGet[0], engines[enIdx] + "");
			else
				keyValue.put(this.keysToGet[0], keyValue.get(this.keysToGet[0]) + " OR " + engines[enIdx] + "");
		}
		if (enginesList.isEmpty()) {
			keyValue.put(this.keysToGet[0], "");
		}
	}
	
	@Override
	public String getHelp() {
		if(keysToGet == null) {
			return "No help text created for this reactor";
		}
		StringBuilder help = new StringBuilder();
		help.append("Inputs:\n");
		int size = keysToGet.length;
		for(int i = 0; i < size; i++) {
			String key = keysToGet[i];
			help.append("\tinput ").append(i).append(":\t").append(key);
			String description = getDescriptionForKey(key);
			if(description != null) {
				help.append(" =\t").append(description);
			}
			help.append("\n");
		}
		return help.toString();
	}
	
	/**
	 * Default is to grab keys from our standardized set
	 * But users can override this method to append their own descriptions for keys
	 * when they are not-standard / are extremely unique for their function
	 * @param key
	 * @return
	 */
	protected String getDescriptionForKey(String key) {
		return ReactorKeysEnum.getDescriptionFromKey(key);
	}
	
	@Override
	public void setPixel(String operation, String fullOperation) {
		this.operationName = operation;
		this.signature = fullOperation;
		this.originalSignature = signature;
	}
	
	@Override
	public void In() {
        curNoun("all");
	}
	
	@Override
	public Object Out() {
		return this.parentReactor;
	}
	
	@Override
	public void setInsight(Insight insight) {
		this.insight = insight;
	}

	@Override
	public String[] getPixel() {
		String [] output = new String[3];
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
	public void setPixelPlanner(PixelPlanner planner) {
		this.planner = planner;
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
		this.childReactor.add(childReactor);
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
	
	public void modifySignatureFromLambdas() {
		List <NounMetadata> lamList = curRow.getNounsOfType(PixelDataType.LAMBDA);
		// replace all the values that is inside this. this could be a recursive call
		for(int lamIndex = 0;lamIndex < lamList.size();lamIndex++) {
			NounMetadata thisLambdaMeta = lamList.get(lamIndex);
			IReactor thisReactor = (IReactor)thisLambdaMeta.getValue();
			String rSignature = thisReactor.getOriginalSignature();
//			String rSignature = thisReactor.getSignature();
			NounMetadata result = thisReactor.execute();// this might further trigger other things

			//The result of a reactor could be a var, if we are doing string replacing we should just replace with the value of that var instead of the var itself
			//reason being the variable is not initialized in the java runtime class through this flow
			if(result.getNounType() == PixelDataType.COLUMN) {
				NounMetadata resultValue = planner.getVariableValue(result.getValue().toString());
				if(resultValue != null) {
					result = resultValue;
				}
			}
			// for compilation reasons
			// if we have a double
			// we dont want it to print with the exponential
			Object replaceValue = result.getValue();
			PixelDataType replaceType = result.getNounType();
			if(replaceType == PixelDataType.CONST_DECIMAL || 
					replaceType == PixelDataType.CONST_INT) {
				// big decimal is easiest way i have seen to do this formatting
				replaceValue = new BigDecimal( ((Number) replaceValue).doubleValue()).toPlainString();
			} else {
				replaceValue = replaceValue + "";
			}
			modifySignature(rSignature, replaceValue.toString());
		}
	}
	
	@Override
	public void modifySignature(String stringToFind, String stringReplacement) {
		LOGGER.debug("Original signature value = " + this.signature);
		this.signature = StringUtils.replaceOnce( this.signature, stringToFind, stringReplacement);
		LOGGER.debug("New signature value = " + this.signature);
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
	
	@Override
	public void mergeUp() {
		// need to figure out the best way to do this
		// i dont want to always just push everything
		// ... not sure if that is avoidable
		
		
		// but i know for filter statements
		// children definitely need to be pushed up
		// based on the curnoun the filter has set
		// we definitely need to define based on the type
		
		if(parentReactor != null) {
			NounMetadata data = new NounMetadata(this, PixelDataType.LAMBDA);
			this.parentReactor.getCurRow().add(data);
		}
	}
	
	@Override
	public List<NounMetadata> getInputs() {
		List<NounMetadata> inputs = new Vector<NounMetadata>();
		// grab all the nouns in the noun store
		Set<String> nounKeys = this.getNounStore().nounRow.keySet();
		for(String nounKey : nounKeys) {
			// grab the genrowstruct for the noun
			// and add its vector to the inputs list
			GenRowStruct struct = this.getNounStore().getNoun(nounKey);
			inputs.addAll(struct.vector);
		}
		
		// we also need to account for some special cases
		// when we have a filter reactor
		// it doesn't get added as an op
		// so we need to go through the child of this reactor
		// and if it has a filter
		// add its nouns to the inputs for this reactor
		for(IReactor child : childReactor) {
			List<NounMetadata> childInputs = child.getInputs();
			if(childInputs != null) {
				inputs.addAll(childInputs);
			}
		}

		return inputs;
	}
	
	@Override
	public List<NounMetadata> getOutputs() {
		//Default operation for the abstract is to return the asName aliases as the outputs
		if(this.asName != null) {
			List<NounMetadata> outputs = new Vector<>();
			for(String alias : this.asName) {
				NounMetadata aliasNoun = new NounMetadata(alias, PixelDataType.ALIAS);
				outputs.add(aliasNoun);
			}
			return outputs;
		}
		return null;
	}
	
	@Override
	public void updatePlan() {
		// at this point, the inner child is still just part of a 
		// larger parent
		// we only need to update the plan when the full pixel
		// is loaded
		if(this.parentReactor != null) {
			return;
		}
		// get the inputs and outputs
		// and add this to the plan using the signature
		// of the reactor
		List<NounMetadata> inputs = this.getInputs();
		List<NounMetadata> outputs = this.getOutputs();

		if(inputs != null) {
			List<String> strInputs = new Vector<String>();
			for(int inputIndex = 0; inputIndex < inputs.size(); inputIndex++) {
				
				NounMetadata noun = inputs.get(inputIndex);
				PixelDataType type = noun.getNounType();
				if(type == PixelDataType.COLUMN) {
					strInputs.add(noun.getValue() + "");
				}
			}
			
			this.planner.addInputs(this.signature, strInputs, TYPE.FLATMAP);
		}
		
		if(outputs != null) {
			List<String> strOutputs = new Vector<String>();
			for(int outputIndex = 0; outputIndex < outputs.size(); outputIndex++) {
				NounMetadata noun = outputs.get(outputIndex);
				PixelDataType type = noun.getNounType();
				if(type == PixelDataType.COLUMN) {
					strOutputs.add(noun.getValue() + "");
				}
			}
			
			this.planner.addOutputs(this.signature, strOutputs, TYPE.FLATMAP);
		}
		
		// if no input or outputs
		// no need to add this to the plan
//		if(inputs != null || outputs != null) {
//			// store the reactor itself onto the planner
//			this.planner.addProperty(this.signature, PixelPlanner.REACTOR_CLASS, this);
//		}
	}
	
	@Override
	public String getSignature() {
		return this.signature;
	}
	
	@Override
	public String getOriginalSignature() {
		return this.originalSignature;
	}
	
	@Override
	public Logger getLogger(String className)
	{
		NounMetadata job = planner.getVariable(JobReactor.JOB_KEY);
		if(job != null) {
			this.jobId = job.getValue() +"";
			Logger retLogger = new InMemoryConsole(this.jobId, className);
			return retLogger;
		} else {
			return LogManager.getLogger(className);
		}
	}
	
	/**
	 * Get the session id set in the job
	 * @return
	 */
	protected String getSessionId() {
		NounMetadata session = planner.getVariable(JobReactor.SESSION_KEY);
		if(session != null) {
			return session.getValue() +"";
		}
		return null;
	}
	
	public void checkMin(int numKey) {
		// checks to see if the minmum number of keys are present else
		// will throw an error
		if(keyValue.size() < numKey) {
			Hashtable details = new Hashtable();
			details.put("error", "Parameters are not present: was expecting " + numKey + " but found only " + keyValue.size());
			details.put("message", "please try help on this command for more details");
			throwLoginError(details);
		}
	}
	
	/**
	 * 
	 * @param details
	 */
	public void throwLoginError(Map details) {
		NounMetadata  noun = new NounMetadata(details, PixelDataType.ERROR, PixelOperationType.LOGGIN_REQUIRED_ERROR, PixelOperationType.ERROR);
		SemossPixelException exception = new SemossPixelException(noun);
		exception.setContinueThreadOfExecution(false);
		throw exception;
	}
	
}