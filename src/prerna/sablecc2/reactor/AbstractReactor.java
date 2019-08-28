package prerna.sablecc2.reactor;

import java.math.BigDecimal;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Hashtable;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Vector;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.codehaus.plexus.util.StringUtils;

import prerna.auth.utils.AbstractSecurityUtils;
import prerna.auth.utils.SecurityAppUtils;
import prerna.auth.utils.SecurityQueryUtils;
import prerna.engine.api.IHeadersDataRow;
import prerna.nameserver.utility.MasterDatabaseUtility;
import prerna.om.Insight;
import prerna.om.ThreadStore;
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
	// get the directory separator
	public static final String DIR_SEPARATOR = java.nio.file.FileSystems.getDefault().getSeparator();
	
	protected Insight insight = null;
	protected PixelPlanner planner = null;

	// keep track of parent reactor and all children reactors
	protected IReactor parentReactor = null;
	protected Vector<IReactor> childReactor = new Vector<IReactor>();

	// keep track of the original signature passed in
	// and the signature when we do basic replacements in the pixel
	protected String signature = null;
	protected String originalSignature = null;
	protected String operationName = null;

	protected String curNoun = null;
	
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
	
	// all the different keys to get
	public String[] keysToGet = new String[]{"no keys defined"};
	// which of these are optional : 1 means required, 0 means optional
	public int[] keyRequired = null;
	
	// defaults if one exists
	// this I am not so sure.. but let us try
	public Object[] keyDefaults = new Object[]{};
	public Map<String, String> keyValue = new Hashtable<String, String>();
	
	public AbstractReactor() {
		
	}
	
	/////////////////////////////////////////////////////////////////////////////
	/////////////////////////////////////////////////////////////////////////////
	/////////////////////////////////////////////////////////////////////////////
	
	/*
	 * Methods for merging up the noun store across reactors
	 */
	
	@Override
	public void mergeUp() {
		// merge this reactor into the parent reactor
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
		
		// this is me testing on 8/14
		// not going to push this yet... will come back later to 
		// figuring out the right logic for this
		
//		// since we use reactors to capture state at various points
//		// like a reactor to generate a Map properly, etc.
//		// there are not really ones we need to put in the plan
//		// since they are in reality just constants
//		if(this.signature == null) {
//			return;
//		}
		
		// get the inputs and outputs
		// and add this to the plan using the signature
		// of the reactor
		List<NounMetadata> inputs = this.getInputs();
		List<NounMetadata> outputs = this.getOutputs();

		if(inputs != null && !inputs.isEmpty()) {
			// this is me testing on 8/14
			// not going to push this yet... will come back later to 
			// figuring out the right logic for this
//			this.planner.addInputs(this.signature, inputs);
			
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
		
		if(outputs != null && !outputs.isEmpty()) {
			// this is me testing on 8/14
			// not going to push this yet... will come back later to 
			// figuring out the right logic for this
//			this.planner.addOutputs(this.signature, outputs);
			
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
	}
	
	/////////////////////////////////////////////////////////////////////////////
	/////////////////////////////////////////////////////////////////////////////
	/////////////////////////////////////////////////////////////////////////////
	
	/*
	 * Noun store methods
	 */
	
	@Override
	public void In() {
        curNoun("all");
	}
	
	@Override
	public Object Out() {
		return this.parentReactor;
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
	
	// get noun
	protected GenRowStruct makeNoun(String noun) {
		GenRowStruct newRow = null;
		getNounStore();
		newRow = store.makeNoun(noun);
		return newRow;
	}
	
	@Override
	public NounStore getNounStore() {
		// I need to see if I have a child
		// if the child
		if(this.store == null) {
			store = new NounStore(operationName);
		}
		return store;
	}
	
	@Override
	public void setNounStore(NounStore store) {
		this.store = store;
	}
	
	/////////////////////////////////////////////////////////////////////////////
	/////////////////////////////////////////////////////////////////////////////
	/////////////////////////////////////////////////////////////////////////////
	
	/*
	 * General Setters / Getters
	 */
	
	@Override
	public void setPixel(String operation, String fullOperation) {
		this.operationName = operation;
		this.signature = fullOperation;
		this.originalSignature = fullOperation;
	}
	
	@Override
	public void setInsight(Insight insight) {
		this.insight = insight;
	}
	
	@Override
	public String[] getPixel() {
		return new String[]{operationName, signature};
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
	public void setChildReactor(IReactor childReactor) {
		this.childReactor.add(childReactor);
	}
	
	@Override
	public List<IReactor> getChildReactors() {
		return this.childReactor;
	}
	
	@Override
	public void setName(String reactorName) {
		this.reactorName = reactorName;
	}
	
	@Override
	public String getName() {
		if(reactorName.isEmpty()) {
			reactorName = this.getClass().getName().replace("Reactor", "");
		}
		return this.reactorName;
	}
	
	@Override
	public void setPixelPlanner(PixelPlanner planner) {
		this.planner = planner;
	}
	
	@Override
	public PixelPlanner getPixelPlanner() {
		return this.planner;
	}
	
	@Override
	public String getSignature() {
		return this.signature;
	}
	
	@Override
	public String getOriginalSignature() {
		return this.originalSignature;
	}
	
	/////////////////////////////////////////////////////////////////////////////
	/////////////////////////////////////////////////////////////////////////////
	/////////////////////////////////////////////////////////////////////////////
	
	/*
	 * Utility methods
	 */
	
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
	public Logger getLogger(String className) {
		String jobId = ThreadStore.getJobId();
		if(jobId != null) {
			this.jobId = jobId;
			Logger retLogger = new InMemoryConsole(this.jobId, className);
			return retLogger;
		}
		
		return LogManager.getLogger(className);
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
		return ThreadStore.getSessionId();
	}
	
	/**
	 * Test the app id and grab the correct value and check if the user needs edit access or just read access
	 * @param appId			String the app id to test
	 * @param edit			Boolean true means the user needs edit access
	 * @return
	 */
	protected String testAppId(String appId, boolean edit) {
		String testId = appId;
		if(AbstractSecurityUtils.securityEnabled()) {
			testId = SecurityQueryUtils.testUserEngineIdForAlias(this.insight.getUser(), testId);
			if(edit) {
				// need edit permission
				if(!SecurityAppUtils.userCanEditEngine(this.insight.getUser(), testId)) {
					throw new IllegalArgumentException("App " + appId + " does not exist or user does not have access to app");
				}
			} else {
				// just need read access
				if(!SecurityAppUtils.userCanViewEngine(this.insight.getUser(), testId)) {
					throw new IllegalArgumentException("App " + appId + " does not exist or user does not have access to app");
				}
			}
		} else {
			testId = MasterDatabaseUtility.testEngineIdIfAlias(testId);
			if(!MasterDatabaseUtility.getAllEngineIds().contains(testId)) {
				throw new IllegalArgumentException("App " + appId + " does not exist");
			}
		}
		return testId;
	}
	
	/////////////////////////////////////////////////////////////////////////////
	/////////////////////////////////////////////////////////////////////////////
	/////////////////////////////////////////////////////////////////////////////
	
	/*
	 * Methods to quickly retrieve inputs in the noun store
	 */
	
	/**
	 * Convenience method to allow order or named noun for basic string inputs
	 */
	public void organizeKeys() {
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
		
		// check which of these are optional
		checkOptional();
	}
	
	/**
	 * Check which inputs are optional or required and throw error if all required are not defined
	 */
	private void checkOptional() {
		StringBuilder nullMessage = new StringBuilder();
		for(int keyIndex = 0;keyRequired != null && keyIndex < keyRequired.length;keyIndex++) {
			int required = keyRequired[keyIndex];
			if(required == 1) {
				String thisKey = keysToGet[keyIndex];
				if(!keyValue.containsKey(thisKey)) {
					// this is where the default would come in
					nullMessage.append(thisKey).append("  ");
				}
			}
		}
		
		if(nullMessage.length() != 0) {
			nullMessage.append("Cannot be empty").insert(0, "Fields  ");
			throw new IllegalArgumentException(nullMessage.toString());
		}
	}

	public void checkMin(int numKey) {
		// checks to see if the minmum number of keys are present else
		// will throw an error
		if(keyValue.size() < numKey) {
			Map<String, String> details = new HashMap<String, String>();
			details.put("error", "Parameters are not present: was expecting " + numKey + " but found only " + keyValue.size());
			details.put("message", "please try help on this command for more details");
			throwLoginError(details);
		}
	}
	
	/////////////////////////////////////////////////////////////////////////////
	/////////////////////////////////////////////////////////////////////////////
	/////////////////////////////////////////////////////////////////////////////

	/*
	 * Throwing common errors
	 */
	
	/**
	 * Throw error since anonymous user
	 */
	public void throwAnonymousUserError() {
		SemossPixelException exception = new SemossPixelException(NounMetadata.getErrorNounMessage("Must be logged in to perform this operation", PixelOperationType.ANONYMOUS_USER_ERROR));
		exception.setContinueThreadOfExecution(false);
		throw exception;
	}
	
	/**
	 * Throw error since user doesn't have access to publish
	 */
	public void throwUserNotPublisherError() {
		SemossPixelException exception = new SemossPixelException(NounMetadata.getErrorNounMessage("User does not have access to publish apps"));
		exception.setContinueThreadOfExecution(false);
		throw exception;
	}
	
	/**
	 * Throw login required error
	 * @param details
	 */
	public void throwLoginError(Map details) {
		SemossPixelException exception = new SemossPixelException(NounMetadata.getErrorNounMessage(details, PixelOperationType.LOGGIN_REQUIRED_ERROR));
		exception.setContinueThreadOfExecution(false);
		throw exception;
	}
	
	
	/////////////////////////////////////////////////////////////////////////////
	
	/*
	 * Methods in interface that are not really used at the moment...
	 */

	@Override
	public void setAs(String [] asName) {
		this.asName = asName;
		// need to set this up on planner as well
		outputFields = new Vector<String>();
		outputFields.addAll(Arrays.asList(asName));
		// re add this to output
		planner.addOutputs(signature, outputFields, type);
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
	public TYPE getType() {
		Object typeProp = getProp("type");
		if(typeProp != null && ((String)typeProp).contains("reduce")) {
			this.type = TYPE.REDUCE;
		}
		return this.type;
	}
	
	@Override
	public STATUS getStatus() {
		return this.status;
	}
	
	@Override
	public IHeadersDataRow map(IHeadersDataRow row) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Object reduce(Iterator iterator) {
		// TODO Auto-generated method stub
		return null;
	}
	
}