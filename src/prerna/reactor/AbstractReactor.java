/*******************************************************************************
 * Copyright 2015 Defense Health Agency (DHA)
 *
 * If your use of this software does not include any GPLv2 components:
 * 	Licensed under the Apache License, Version 2.0 (the "License");
 * 	you may not use this file except in compliance with the License.
 * 	You may obtain a copy of the License at
 *
 * 	  http://www.apache.org/licenses/LICENSE-2.0
 *
 * 	Unless required by applicable law or agreed to in writing, software
 * 	distributed under the License is distributed on an "AS IS" BASIS,
 * 	WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * 	See the License for the specific language governing permissions and
 * 	limitations under the License.
 * ----------------------------------------------------------------------------
 * If your use of this software includes any GPLv2 components:
 * 	This program is free software; you can redistribute it and/or
 * 	modify it under the terms of the GNU General Public License
 * 	as published by the Free Software Foundation; either version 2
 * 	of the License, or (at your option) any later version.
 *
 * 	This program is distributed in the hope that it will be useful,
 * 	but WITHOUT ANY WARRANTY; without even the implied warranty of
 * 	MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * 	GNU General Public License for more details.
 *******************************************************************************/
package prerna.reactor;

import java.math.BigDecimal;
import java.time.LocalDateTime;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.commons.lang3.Strings;
import org.apache.commons.text.StringSubstitutor;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.json.JSONArray;
import org.json.JSONObject;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.ToNumberPolicy;
import com.google.gson.reflect.TypeToken;

import prerna.algorithm.api.ITableDataFrame;
import prerna.auth.User;
import prerna.auth.utils.AbstractSecurityUtils;
import prerna.auth.utils.SecurityEngineUtils;
import prerna.auth.utils.SecurityProjectUtils;
import prerna.auth.utils.SecurityQueryUtils;
import prerna.engine.api.IEngine;
import prerna.engine.api.IHeadersDataRow;
import prerna.om.Insight;
import prerna.om.ThreadStore;
import prerna.project.api.IProject;
import prerna.reactor.agent.mcp.MCPUtility;
import prerna.reactor.agent.mcp.MCPUtility.MCPExecution;
import prerna.sablecc2.comm.InMemoryConsole;
import prerna.sablecc2.om.GenRowStruct;
import prerna.sablecc2.om.NounStore;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.execptions.SemossPixelException;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.security.TypeReference;
import prerna.util.gson.LocalDateTimeAdapter;
import prerna.util.gson.ZonedDateTimeAdapter;

public abstract class AbstractReactor implements IReactor {

	protected static final Gson GSON = new GsonBuilder().disableHtmlEscaping()
			.setObjectToNumberStrategy(ToNumberPolicy.LONG_OR_DOUBLE)
			.registerTypeAdapter(LocalDateTime.class, new LocalDateTimeAdapter())
			.registerTypeAdapter(ZonedDateTime.class, new ZonedDateTimeAdapter()).create();

	private static final Logger classLogger = LogManager.getLogger(AbstractReactor.class);
	// get the directory separator
	public static final String DIR_SEPARATOR = "/";
	protected static final String ALL_NOUN_STORE = "all";

	protected Insight insight = null;
	protected PixelPlanner planner = null;

	// keep track of parent reactor and all children reactors
	protected IReactor parentReactor = null;
	protected List<IReactor> childReactor = Collections.synchronizedList(new ArrayList<IReactor>());

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
	protected String[] asName = null;
	protected List<String> outputFields = null;
	protected List<String> outputTypes = null;

	@Deprecated
	protected Map<String, Object> propStore = new ConcurrentHashMap<>();

	protected Lambda runner = null;

	protected String[] defaultOutputAlias;
	protected boolean evaluate = false;

	// all the different keys to get
	public String[] keysToGet = new String[] {};
	// which of these are optional : 1 means required, 0 means optional
	protected int[] keyRequired = null;
	// single or multi if 1 multi if 0 single
	protected int[] keyMulti = null;

	// defaults if one exists
	// this I am not so sure.. but let us try
	protected Object[] keyDefaults = new Object[] {};
	public Map<String, String> keyValue = new ConcurrentHashMap<>();

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
		if (parentReactor != null) {
			NounMetadata data = new NounMetadata(this, PixelDataType.LAMBDA);
			this.parentReactor.getCurRow().add(data);
		}
	}

	@Override
	public List<NounMetadata> getInputs() {
		List<NounMetadata> inputs = Collections.synchronizedList(new ArrayList<NounMetadata>());
		// grab all the nouns in the noun store
		Set<String> nounKeys = this.getNounStore().nounRow.keySet();
		for (String nounKey : nounKeys) {
			// grab the genrowstruct for the noun
			// and add its vector to the inputs list
			GenRowStruct struct = this.getNounStore().getGenRowStruct(nounKey);
			inputs.addAll(struct.vector);
		}

		// we also need to account for some special cases
		// when we have a filter reactor
		// it doesn't get added as an op
		// so we need to go through the child of this reactor
		// and if it has a filter
		// add its nouns to the inputs for this reactor
		for (IReactor child : childReactor) {
			List<NounMetadata> childInputs = child.getInputs();
			if (childInputs != null) {
				inputs.addAll(childInputs);
			}
		}

		return inputs;
	}

	@Override
	public List<NounMetadata> getOutputs() {
		// Default operation for the abstract is to return the asName aliases as the
		// outputs
		if (this.asName != null) {
			List<NounMetadata> outputs = Collections.synchronizedList(new ArrayList<NounMetadata>());
			for (String alias : this.asName) {
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
		if (this.parentReactor != null) {
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

		if (inputs != null && !inputs.isEmpty()) {
			// this is me testing on 8/14
			// not going to push this yet... will come back later to
			// figuring out the right logic for this
//			this.planner.addInputs(this.signature, inputs);

			List<String> strInputs = new ArrayList<String>();
			for (int inputIndex = 0; inputIndex < inputs.size(); inputIndex++) {

				NounMetadata noun = inputs.get(inputIndex);
				PixelDataType type = noun.getNounType();
				if (type == PixelDataType.COLUMN) {
					strInputs.add(noun.getValue() + "");
				}
			}
			this.planner.addInputs(this.signature, strInputs, TYPE.FLATMAP);
		}

		if (outputs != null && !outputs.isEmpty()) {
			// this is me testing on 8/14
			// not going to push this yet... will come back later to
			// figuring out the right logic for this
//			this.planner.addOutputs(this.signature, outputs);

			List<String> strOutputs = Collections.synchronizedList(new ArrayList<String>());
			for (int outputIndex = 0; outputIndex < outputs.size(); outputIndex++) {
				NounMetadata noun = outputs.get(outputIndex);
				PixelDataType type = noun.getNounType();
				if (type == PixelDataType.COLUMN) {
					strOutputs.add(noun.getValue() + "");
				}
			}

			this.planner.addOutputs(this.signature, strOutputs, TYPE.FLATMAP);
		}
	}

	@Override
	public Map<String, List<Map>> getStoreMap() {
		Map<String, List<Map>> inputMap = new HashMap<>();

		for (int keyIndex = 0; keyIndex < keysToGet.length; keyIndex++) {
			String key = keysToGet[keyIndex];
			if (this.store.getGenRowStruct(key) != null) {
				GenRowStruct grs = this.store.getGenRowStruct(key);
				if (!grs.isEmpty()) {
					List<Map> inputVals = new ArrayList<>();
					for (int i = 0; i < grs.size(); i++) {
						inputVals.add(processNounMetadata(grs.getNoun(i)));
					}
					inputMap.put(keysToGet[keyIndex], inputVals);
				}
			}
		}

		// fill in order based on whatever is left
		int counter = 0;
		if (this.curRow != null && !this.curRow.isEmpty()) {
			for (int keyIndex = 0; keyIndex < keysToGet.length; keyIndex++) {
				if (!inputMap.containsKey(keysToGet[keyIndex])) {
					List<Map> singleObj = new ArrayList<>();
					singleObj.add(processNounMetadata(this.curRow.getNoun(counter)));
					inputMap.put(keysToGet[keyIndex], singleObj);
					// increase counter index
					counter++;
				}

				if (counter >= this.curRow.size()) {
					break;
				}
			}
		}

		// now go through the noun store for anything else
		// in case the keysToGet is not accurate
		for (String nounKey : this.getNounStore().getNounKeys()) {
			if (nounKey.equals("all")) {
				continue;
			}
			if (!inputMap.containsKey(nounKey)) {
				GenRowStruct grs = this.store.getGenRowStruct(nounKey);
				if (!grs.isEmpty()) {
					List<Map> inputVals = new ArrayList<>();
					for (int i = 0; i < grs.size(); i++) {
						inputVals.add(processNounMetadata(grs.getNoun(i)));
					}
					inputMap.put(nounKey, inputVals);
				}
			}
		}

		return inputMap;
	}

	/**
	 * Get a json friendly version of the noun metadata
	 * 
	 * @param noun
	 * @return
	 */
	public Map<String, Object> processNounMetadata(NounMetadata noun) {
		PixelDataType type = noun.getNounType();
		if (type == PixelDataType.LAMBDA) {
			return processLambdaNounMap(noun);
		} else if (type == PixelDataType.FRAME) {
			return processFrameNounMap(noun);
		} else {
			return processBasicNounMap(noun);
		}
	}

	private Map<String, Object> processLambdaNounMap(NounMetadata noun) {
		Map<String, Object> lambdaMap = new HashMap<>();
		lambdaMap.put("type", noun.getNounType().getKey());
		// the value is another PixelOperation
		lambdaMap.put("value", ((IReactor) noun.getValue()).getStoreMap());
		return lambdaMap;
	}

	private Map<String, Object> processBasicNounMap(NounMetadata noun) {
		Map<String, Object> basicInput = new HashMap<>();
		basicInput.put("type", noun.getNounType().getKey());
		basicInput.put("value", noun.getValue());
		return basicInput;
	}

	private Map<String, Object> processFrameNounMap(NounMetadata noun) {
		if (noun.getOpType().contains(PixelOperationType.FRAME_MAP)) {
			return processBasicNounMap(noun);
		}
		Map<String, Object> frameMap = new HashMap<>();
		ITableDataFrame frame = (ITableDataFrame) noun.getValue();
		frameMap.put(ReactorKeysEnum.FRAME_TYPE.getKey(), frame.getFrameType().getTypeAsString());
		String name = frame.getOriginalName();
		if (name != null) {
			frameMap.put(PixelDataType.ALIAS.getKey(), name);
			if (!name.equals(frame.getName())) {
				frameMap.put("queryName", frame.getName());
			}
		}

		Map<String, Object> nounStructure = new HashMap<>();
		nounStructure.put("type", PixelDataType.FRAME_MAP.getKey());
		nounStructure.put("value", frameMap);
		return nounStructure;
	}

	/////////////////////////////////////////////////////////////////////////////
	/////////////////////////////////////////////////////////////////////////////
	/////////////////////////////////////////////////////////////////////////////

	/*
	 * Noun store methods
	 */

	@Override
	public void In() {
		curNoun(ALL_NOUN_STORE);
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
		curRow = store.getGenRowStruct(ALL_NOUN_STORE);
	}

	// get noun
	protected GenRowStruct makeNoun(String noun) {
		GenRowStruct newRow = null;
		getNounStore();
		newRow = store.makeGenRowStruct(noun);
		return newRow;
	}

	@Override
	public NounStore getNounStore() {
		// I need to see if I have a child
		// if the child
		if (this.store == null) {
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
		return new String[] { operationName, signature };
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
		if (reactorName.isEmpty()) {
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

	@Override
	public void modifySignatureFromLambdas() {
		List<NounMetadata> lamList = curRow.getNounsOfType(PixelDataType.LAMBDA);
		// replace all the values that is inside this. this could be a recursive call
		for (int lamIndex = 0; lamIndex < lamList.size(); lamIndex++) {
			NounMetadata thisLambdaMeta = lamList.get(lamIndex);
			IReactor thisReactor = (IReactor) thisLambdaMeta.getValue();
			String rSignature = thisReactor.getOriginalSignature();
//			String rSignature = thisReactor.getSignature();
			NounMetadata result = thisReactor.execute();// this might further trigger other things

			// The result of a reactor could be a var, if we are doing string replacing we
			// should just replace with the value of that var instead of the var itself
			// reason being the variable is not initialized in the java runtime class
			// through this flow
			if (result.getNounType() == PixelDataType.COLUMN) {
				NounMetadata resultValue = planner.getVariableValue(result.getValue().toString());
				if (resultValue != null) {
					result = resultValue;
				}
			}
			// for compilation reasons
			// if we have a double
			// we dont want it to print with the exponential
			Object replaceValue = result.getValue();
			PixelDataType replaceType = result.getNounType();
			if (replaceType == PixelDataType.CONST_DECIMAL || replaceType == PixelDataType.CONST_INT) {
				// big decimal is easiest way i have seen to do this formatting
				replaceValue = BigDecimal.valueOf(((Number) replaceValue).doubleValue()).toPlainString();
			} else {
				replaceValue = replaceValue + "";
			}
			modifySignature(rSignature, replaceValue.toString());
		}
	}

	@Override
	public void modifySignature(String stringToFind, String stringReplacement) {
		classLogger.debug("Original signature value = " + this.signature);
		this.signature = Strings.CS.replaceOnce(this.signature, stringToFind, stringReplacement);
		classLogger.debug("New signature value = " + this.signature);
	}

	@Override
	public String getReactorDescription() {
		return null;
	}

	/**
	 * Override in child reactors to define the output schema. Returns null by
	 * default, meaning no output schema is defined.
	 */
	public JSONObject getResponseSchema() {
		return null;
	}

	@Override
	public String getHelp() {
		if (keysToGet == null) {
			return "No help text created for this reactor";
		}
		StringBuilder help = new StringBuilder();
		String overallDescription = getReactorDescription();
		if (overallDescription == null) {
			overallDescription = "No description present";
		}
		help.append("Description:\n").append(overallDescription).append("\n");
		help.append("Inputs:\n");
		int size = keysToGet.length;
		for (int i = 0; i < size; i++) {
			String keyDefinition = keysToGet[i];
			String canonicalKey = keyDefinition.split(",")[0];
			String helpKeyName = keyDefinition.replace(",", "/");
			help.append("\tinput ").append(i).append(":\t").append(helpKeyName);
			String description = getDescriptionForKey(canonicalKey);
			if (description == null) {
				description = "No description present";
			}
			help.append(" =\t").append(description);
			help.append("\n");
		}
		help.append("\nMCP Schema:\n");
		help.append(this.asMcpTool().toString(4));

		JSONObject responseSchema = getResponseSchema();
		if (responseSchema != null) {
			help.append("\nResponse Schema:\n");
			help.append(responseSchema.toString(4));
		}

		return help.toString();
	}

	/**
	 * Default is to grab keys from our standardized set But users can override this
	 * method to append their own descriptions for keys when they are not-standard /
	 * are extremely unique for their function
	 * 
	 * @param key
	 * @return
	 */
	protected String getDescriptionForKey(String key) {
		return ReactorKeysEnum.getDescriptionFromKey(key);
	}

	@Override
	public Logger getLogger(String className) {
		return getLogger(className, false);
	}

	@Override
	public Logger getLogger(String className, boolean partial) {
		String jobId = ThreadStore.getJobId();
		if (jobId != null) {
			InMemoryConsole retLogger = new InMemoryConsole(className, ThreadStore.getJobId());
			retLogger.setPartial(partial);
			return retLogger;
		}

		return LogManager.getLogger(className);
	}

	/**
	 * Get the session id set in the job
	 * 
	 * @return
	 */
	protected String getSessionId() {
		return ThreadStore.getSessionId();
	}

	/**
	 * Get the session id for the insight
	 * 
	 * @return
	 */
	protected String getRouteId() {
		return ThreadStore.getRouteId();
	}

	/**
	 * Test the app id and grab the correct value and check if the user needs edit
	 * access or just read access
	 * 
	 * @param databaseId String the database id to test
	 * @param edit       Boolean true means the user needs edit access
	 * @return
	 */
	protected String testDatabaseId(String databaseId, boolean edit) {
		String testId = databaseId;
		testId = SecurityQueryUtils.testUserEngineIdForAlias(this.insight.getUser(), testId);
		if (edit) {
			// need edit permission
			if (!SecurityEngineUtils.userCanEditEngine(this.insight.getUser(), testId)) {
				throw new IllegalArgumentException(
						"Database " + databaseId + " does not exist or user does not have access to the database");
			}
		} else {
			// just need read access
			if (!SecurityEngineUtils.userCanViewEngine(this.insight.getUser(), testId)) {
				throw new IllegalArgumentException(
						"Database " + databaseId + " does not exist or user does not have access to the database");
			}
		}
		return testId;
	}

	/**
	 * 
	 * @param engine
	 * @param engineId
	 * @param user
	 */
	protected void checkEngineEditSecurity(IEngine engine, User user) {
		if (engine == null) {
			throw new NullPointerException("Engine/Project is null");
		}
		if (AbstractSecurityUtils.anonymousUsersEnabled() && user.isAnonymous()) {
			throwAnonymousUserError();
		}

		if (engine instanceof IProject) {
			if (!SecurityProjectUtils.userCanViewProject(user, engine.getEngineId())) {
				throw new IllegalArgumentException(
						"Project " + engine.getEngineId() + " does not exist or user does not have access");
			}
		} else {
			if (!SecurityEngineUtils.userCanViewEngine(user, engine.getEngineId())) {
				throw new IllegalArgumentException(
						"Engine " + engine.getEngineId() + " does not exist or user does not have access");
			}
		}
	}

	/////////////////////////////////////////////////////////////////////////////
	/////////////////////////////////////////////////////////////////////////////
	/////////////////////////////////////////////////////////////////////////////

	/*
	 * Methods to quickly retrieve inputs in the noun store
	 */

	/**
	 * Convenience method to allow order-based or named noun inputs. This method
	 * populates the reactor's internal `keyValue` map from the `NounStore`. It
	 * supports aliasing for keys defined in `keysToGet` using a comma-separated
	 * format (e.g., "key,alias1,alias2"). The first key in the definition is
	 * treated as the canonical key.
	 */
	protected void organizeKeys() {
		// First, process named nouns from the store
		if (this.getNounStore().size() > 0) {
			for (String keyDefinition : keysToGet) {
				String[] keyAliases = keyDefinition.split(",");
				String canonicalKey = keyAliases[0];

				// Don't process if we already found a value for this canonical key
				if (keyValue.containsKey(canonicalKey)) {
					continue;
				}

				for (String alias : keyAliases) {
					GenRowStruct grs = this.store.getGenRowStruct(alias);
					if (grs != null && !grs.isEmpty()) {
						keyValue.put(canonicalKey, grs.get(0) + "");
						// Found a value, break to the next key definition
						break;
					}
				}
			}
		}

		// Second, fill in missing keys from the ordered curRow
		int counter = 0;
		if (this.curRow != null && !this.curRow.isEmpty()) {
			for (String keyDefinition : keysToGet) {
				String canonicalKey = keyDefinition.split(",")[0];
				if (!keyValue.containsKey(canonicalKey)) {
					if (counter < this.curRow.size()) {
						keyValue.put(canonicalKey, this.curRow.get(counter) + "");
						counter++;
					}
				}
			}
		}

		// Finally, check for required keys
		checkOptional();
	}

	/**
	 * Check which inputs are optional or required and throw an error if any
	 * required inputs are not defined. This method supports aliased keys, providing
	 * a more informative error message.
	 */
	protected void checkOptional() {
		StringBuilder nullMessage = new StringBuilder();
		if (keyRequired != null) {
			for (int i = 0; i < keyRequired.length; i++) {
				if (keyRequired[i] == 1) {
					String keyDefinition = keysToGet[i];
					String canonicalKey = keyDefinition.split(",")[0];
					if (!keyValue.containsKey(canonicalKey)) {
						// For the error message, show all possible aliases
						nullMessage.append(keyDefinition.replace(",", "/")).append(" ");
					}
				}
			}
		}

		if (nullMessage.length() > 0) {
			nullMessage.insert(0, "Required input(s) missing: ");
			throw new IllegalArgumentException(nullMessage.toString().trim());
		}
	}

	public void checkMin(int numKey) {
		// checks to see if the minmum number of keys are present else
		// will throw an error
		if (keyValue.size() < numKey) {
			Map<String, String> details = new HashMap<String, String>();
			details.put("error",
					"Parameters are not present: was expecting " + numKey + " but found only " + keyValue.size());
			details.put("message", "please try help on this command for more details");
			throwLoginError(details);
		}
	}

	/////////////////////////////////////////////////////////////////////////////
	/////////////////////////////////////////////////////////////////////////////
	/////////////////////////////////////////////////////////////////////////////

	/*
	 * MCP
	 */

	@Override
	public JSONObject asMcpTool() {
		JSONObject tool = new JSONObject();
		String name = this.getClass().getSimpleName();
		if (name.endsWith("Reactor")) {
			name = name.substring(0, name.length() - "Reactor".length());
		}
		tool.put("name", name);
		tool.put("title", MCPUtility.formatToTitleCase(name));
		String overallDescription = getReactorDescription();
		if (overallDescription == null) {
			overallDescription = "No description present";
		}
		tool.put("description", overallDescription);
		JSONObject inputSchema = new JSONObject();
		inputSchema.put("properties", getMcpProperties());
		JSONArray required = new JSONArray();
		if (this.keyRequired == null) {
			// assume everything is required ...
			for (String keyToGet : this.keysToGet) {
				required.put(keyToGet);
			}
		} else {
			for (int i = 0; i < this.keyRequired.length; i++) {
				if (this.keyRequired[i] == 1) {
					required.put(this.keysToGet[i]);
				}
			}
		}
		inputSchema.put("required", required);
		inputSchema.put("type", "object");
		inputSchema.put("title", name + "_Arguments");
		tool.put("inputSchema", inputSchema);

		// Read MCP tool metadata if present and populate _meta
		Map<String, String> mcpMeta = getMcpToolMetadata();
		if (mcpMeta != null) {
			JSONObject meta = new JSONObject();
			meta.put(MCPUtility.SMSS_FUNCTION_NAME, name);
			meta.put(MCPUtility.SMSS_MCP_EXECUTION,
					mcpMeta.getOrDefault(MCPUtility.SMSS_MCP_EXECUTION, MCPExecution.AUTO.getValue()));

			JSONObject uiJson = new JSONObject();
			String displayLocation = mcpMeta.get(MCPUtility.UI_DISPLAY_LOCATION);
			if (displayLocation != null && !displayLocation.isEmpty()) {
				uiJson.put(MCPUtility.UI_DISPLAY_LOCATION, displayLocation);
			}
			String loadingMessage = mcpMeta.get(MCPUtility.UI_LOADING_MESSAGE);
			if (loadingMessage != null && !loadingMessage.isEmpty()) {
				uiJson.put(MCPUtility.UI_LOADING_MESSAGE, loadingMessage);
			}
			String resourceURI = mcpMeta.get(MCPUtility.UI_RESOURCE_URI);
			if (resourceURI != null && !resourceURI.isEmpty()) {
				uiJson.put(MCPUtility.UI_RESOURCE_URI, resourceURI);
			}
			meta.put(MCPUtility.SMSS_MCP_UI, uiJson);
			tool.put("_meta", meta);
		}

		return tool;
	}

	/**
	 * Returns MCP tool metadata for this reactor, or {@code null} if this reactor
	 * is not an MCP tool. Reactors that should be discoverable by package scanning
	 * in {@code MakePixelMCPReactor} must override this method and return a
	 * non-null map.
	 * <p>
	 * Supported keys (use {@link MCPUtility} constants):
	 * <ul>
	 * <li>{@code SMSS_MCP_EXECUTION} - "auto", "ask", or "disabled" (defaults to
	 * "auto" if omitted)</li>
	 * <li>{@code displayLocation} - "sidebar", "inline", or "hidden"</li>
	 * <li>{@code loadingMessage} - custom loading text shown during execution</li>
	 * <li>{@code resourceURI} - portal page path for the tool's UI</li>
	 * </ul>
	 *
	 * @return a map of MCP metadata key-value pairs, or {@code null} if not an MCP
	 *         tool
	 */
	public Map<String, String> getMcpToolMetadata() {
		Map<String, String> meta = new HashMap<>();
		// default to auto execution for reactors
		meta.put(MCPUtility.SMSS_MCP_EXECUTION, MCPUtility.MCPExecution.AUTO.getValue());
		// sidebar to view default json for reactor input+output
		meta.put(MCPUtility.UI_DISPLAY_LOCATION, MCPUtility.MCPDisplayOption.SIDEBAR.getValue());
		// keeping below as comments for custom reactors to override, but we assume no
		// UI or message by default
//		meta.put(MCPUtility.UI_LOADING_MESSAGE, "Running tool...");
//		meta.put(MCPUtility.UI_RESOURCE_URI, "index.html");
		return meta;
	}

	/**
	 * Get the reactor parameters and properties to display in the MCP JSON.
	 * 
	 * @return
	 */
	public JSONObject getMcpProperties() {
		JSONObject properties = new JSONObject();
		for (String keyDefinition : this.keysToGet) {
			String[] aliases = keyDefinition.split(",");
			String canonicalKey = aliases[0];

			JSONObject paramMap = new JSONObject();
			paramMap.put("title", canonicalKey);
			paramMap.put("type", getKeyTypeForMCP(canonicalKey).getValue());

			String description = getDescriptionForKey(canonicalKey);
			if (description == null) {
				description = "No description present";
			}

//			// Optionally, add aliases to the description
//			if (aliases.length > 1) {
//				description += " (aliases: " + String.join(", ", Arrays.copyOfRange(aliases, 1, aliases.length)) + ")";
//			}

			paramMap.put("description", description);
			properties.put(canonicalKey, paramMap);
		}
		return properties;
	}

	/**
	 * Represents the specific type for a given key that the reactor is expecting
	 * for MCPs. Reactors should override this to provide more specific information
	 * about types.
	 * 
	 * Default is string, accepted values are: array, boolean, number, integer,
	 * string, object
	 * 
	 * @param key
	 * @return
	 */
	protected MCP_KEY_TYPE getKeyTypeForMCP(String key) {
		if (key.equals(ReactorKeysEnum.MAP.getKey()) || key.equals(ReactorKeysEnum.PARAM_VALUES_MAP.getKey())
				|| key.equals(ReactorKeysEnum.METADATA.getKey())) {
			return MCP_KEY_TYPE.OBJECT;
		} else if (key.equals(ReactorKeysEnum.LIMIT.getKey()) || key.equals(ReactorKeysEnum.OFFSET.getKey())) {
			return MCP_KEY_TYPE.INTEGER;
		}

		return MCP_KEY_TYPE.STRING;
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
	public static void throwAnonymousUserError() {
		SemossPixelException exception = new SemossPixelException(NounMetadata.getErrorNounMessage(
				"Must be logged in to perform this operation", PixelOperationType.ANONYMOUS_USER_ERROR));
		exception.setContinueThreadOfExecution(false);
		throw exception;
	}

	/**
	 * Throw error since user doesn't have access to publish
	 */
	public static void throwUserNotPublisherError() {
		SemossPixelException exception = new SemossPixelException(
				NounMetadata.getErrorNounMessage("User does not have access to publish databases or projects"));
		exception.setContinueThreadOfExecution(false);
		throw exception;
	}

	/**
	 * Throw error if functionality is only provided for admins
	 */
	public static void throwFunctionalityOnlyExposedForAdminsError() {
		SemossPixelException exception = new SemossPixelException(
				NounMetadata.getErrorNounMessage("This functionality is limited to only admins"));
		exception.setContinueThreadOfExecution(false);
		throw exception;
	}

	/**
	 * Throw error since user doesn't have access to export
	 */
	public static void throwUserNotExporterError() {
		SemossPixelException exception = new SemossPixelException(
				NounMetadata.getErrorNounMessage("User does not have access to export data"));
		exception.setContinueThreadOfExecution(false);
		throw exception;
	}

	/**
	 * Throw login required error
	 * 
	 * @param details
	 */
	public static void throwLoginError(Map details) {
		SemossPixelException exception = new SemossPixelException(
				NounMetadata.getErrorNounMessage(details, PixelOperationType.LOGGIN_REQUIRED_ERROR));
		exception.setContinueThreadOfExecution(false);
		throw exception;
	}

	/////////////////////////////////////////////////////////////////////////////

	/*
	 * Methods in interface that are not really used at the moment...
	 */

	@Override
	public void setAs(String[] asName) {
		this.asName = asName;
		// need to set this up on planner as well
		outputFields = Collections.synchronizedList(new ArrayList<String>());
		outputFields.addAll(Arrays.asList(asName));
		// re add this to output
		planner.addOutputs(signature, outputFields, type);
	}

	@Deprecated
	@Override
	public void setProp(String key, Object value) {
		propStore.put(key, value);
	}

	@Deprecated
	@Override
	public Object getProp(String key) {
		return propStore.get(key);
	}

	@Deprecated
	@Override
	public boolean hasProp(String key) {
		return propStore.containsKey(key);
	}

	@Deprecated
	@Override
	public TYPE getType() {
		Object typeProp = getProp("type");
		if (typeProp != null && ((String) typeProp).contains("reduce")) {
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

	@Override
	public boolean canMergeIntoQs() {
		return false;
	}

	@Override
	public Map<String, Object> mergeIntoQsMetadata() {
		return null;
	}

	/**
	 * Gets a success message noun
	 * 
	 * @param message
	 * @return
	 */
	public static NounMetadata getSuccess(String message) {
		return NounMetadata.getSuccessNounMessage(message);
	}

	/**
	 * Returns a error message noun
	 * 
	 * @param message
	 * @return
	 */
	public static NounMetadata getError(String message) {
		return NounMetadata.getErrorNounMessage(message);
	}

	/**
	 * Returns a warning message noun
	 * 
	 * @param message
	 * @return
	 */
	public static NounMetadata getWarning(String message) {
		return NounMetadata.getWarningNounMessage(message);
	}

	/**
	 * Utility method to get the string inputs from a named GenRowStruct entry
	 * 
	 * @param key
	 * @return
	 */
	protected List<String> getNounAsStringList(String key) {
		List<String> columns = new ArrayList<>();
		GenRowStruct colGrs = this.store.getGenRowStruct(key);
		if (colGrs != null && !colGrs.isEmpty()) {
			for (int selectIndex = 0; selectIndex < colGrs.size(); selectIndex++) {
				String column = colGrs.get(selectIndex) + "";
				columns.add(column);
			}
		}

		return columns;
	}

	/**
	 * Get the GenRowStruct for a given key from the noun store.
	 * 
	 * @param key The key to retrieve from the noun store.
	 * @return The GenRowStruct, or null if not found.
	 */
	protected GenRowStruct getGenRowStruct(String key) {
		if (store == null) {
			return null;
		}
		return store.getGenRowStruct(key);
	}

	/**
	 * Get the GenRowStruct for a given index from the noun store.
	 * 
	 * @param index The index of the key to retrieve from the noun store.
	 * @return The GenRowStruct, or null if not found.
	 */
	protected GenRowStruct getGenRowStruct(int index) {
		if (this.store == null || this.keysToGet == null || index >= this.keysToGet.length) {
			return null;
		}
		return store.getGenRowStruct(this.keysToGet[index]);
	}

	/**
	 * Convert all values in a GenRowStruct into string representations.
	 * 
	 * @param grs The input GenRowStruct.
	 * @return A list of string values (empty when input is null/empty).
	 */
	protected List<String> convertAllValuesToString(GenRowStruct grs) {
		List<String> values = new ArrayList<>();
		if (grs == null || grs.isEmpty()) {
			return values;
		}
		for (Object value : grs.getAllValues()) {
			values.add(value == null ? null : value.toString());
		}
		return values;
	}

	/**
	 * Convert all current row values into string representations.
	 * 
	 * @return A list of string values from the current row.
	 */
	protected List<String> getCurRowValuesAsString() {
		return getCurRowValuesAsString(0);
	}

	/**
	 * Get string noun values from curRow (COLUMN and CONST_STRING).
	 * 
	 * @return A list of string noun values from curRow.
	 */
	protected List<String> getCurRowStringValues() {
		if (this.curRow == null || this.curRow.isEmpty()) {
			return new ArrayList<>();
		}
		return this.curRow.getAllStrValues();
	}

	/**
	 * Convert current row values into string representations starting from a given
	 * index.
	 * 
	 * @param startIndex The starting index in the current row.
	 * @return A list of string values from the current row.
	 */
	protected List<String> getCurRowValuesAsString(int startIndex) {
		List<String> values = new ArrayList<>();
		if (this.curRow == null || this.curRow.isEmpty()) {
			return values;
		}
		int safeStartIndex = Math.max(0, startIndex);
		for (int i = safeStartIndex; i < this.curRow.size(); i++) {
			Object value = this.curRow.get(i);
			values.add(value == null ? null : value.toString());
		}
		return values;
	}

	/**
	 * Get the first value from a keyed GenRowStruct entry, falling back to a value
	 * in curRow at the provided index.
	 * 
	 * @param key         The key to retrieve from noun store.
	 * @param curRowIndex The fallback index in curRow.
	 * @return The value, or null when unavailable.
	 */
	protected Object getValueFromKeyOrCurRow(String key, int curRowIndex) {
		GenRowStruct grs = getGenRowStruct(key);
		if (grs != null && !grs.isEmpty()) {
			return grs.get(0);
		}
		if (this.curRow != null && curRowIndex >= 0 && this.curRow.size() > curRowIndex) {
			return this.curRow.get(curRowIndex);
		}
		return null;
	}

	/**
	 * Get the first value from a keyed GenRowStruct entry by key index, falling
	 * back to a value in curRow at the provided index.
	 * 
	 * @param keyIndex    The key index in keysToGet.
	 * @param curRowIndex The fallback index in curRow.
	 * @return The value, or null when unavailable.
	 */
	protected Object getValueFromKeyOrCurRow(int keyIndex, int curRowIndex) {
		GenRowStruct grs = getGenRowStruct(keyIndex);
		if (grs != null && !grs.isEmpty()) {
			return grs.get(0);
		}
		if (this.curRow != null && curRowIndex >= 0 && this.curRow.size() > curRowIndex) {
			return this.curRow.get(curRowIndex);
		}
		return null;
	}

	/**
	 * Get a string by key, falling back to curRow index 0.
	 * 
	 * @param key The key to retrieve from noun store.
	 * @return The string value, or null if not found.
	 */
	protected String getStringFromKeyOrCurRow(String key) {
		return getStringFromKeyOrCurRow(key, 0);
	}

	/**
	 * Get a string by key index, falling back to curRow index 0.
	 * 
	 * @param keyIndex The key index in keysToGet.
	 * @return The string value, or null if not found.
	 */
	protected String getStringFromKeyOrCurRow(int keyIndex) {
		return getStringFromKeyOrCurRow(keyIndex, 0);
	}

	/**
	 * Get a string by key, falling back to the provided curRow index.
	 * 
	 * @param key         The key to retrieve from noun store.
	 * @param curRowIndex The fallback index in curRow.
	 * @return The string value, or null if not found.
	 */
	protected String getStringFromKeyOrCurRow(String key, int curRowIndex) {
		Object value = getValueFromKeyOrCurRow(key, curRowIndex);
		return value != null ? value.toString() : null;
	}

	/**
	 * Get a string by key, falling back to the first string noun value in curRow.
	 * 
	 * @param key The key to retrieve from noun store.
	 * @return The string value, or null if not found.
	 */
	protected String getStringFromKeyOrCurRowStringValue(String key) {
		GenRowStruct grs = getGenRowStruct(key);
		if (grs != null && !grs.isEmpty()) {
			Object value = grs.get(0);
			return value != null ? value.toString() : null;
		}
		List<String> curRowStringValues = getCurRowStringValues();
		if (!curRowStringValues.isEmpty()) {
			return curRowStringValues.get(0);
		}
		return null;
	}

	/**
	 * Get a string by key index, falling back to the first string noun value in
	 * curRow.
	 * 
	 * @param keyIndex The key index in keysToGet.
	 * @return The string value, or null if not found.
	 */
	protected String getStringFromKeyOrCurRowStringValue(int keyIndex) {
		GenRowStruct grs = getGenRowStruct(keyIndex);
		if (grs != null && !grs.isEmpty()) {
			Object value = grs.get(0);
			return value != null ? value.toString() : null;
		}
		List<String> curRowStringValues = getCurRowStringValues();
		if (!curRowStringValues.isEmpty()) {
			return curRowStringValues.get(0);
		}
		return null;
	}

	/**
	 * Get a string by key index, falling back to the provided curRow index.
	 * 
	 * @param keyIndex    The key index in keysToGet.
	 * @param curRowIndex The fallback index in curRow.
	 * @return The string value, or null if not found.
	 */
	protected String getStringFromKeyOrCurRow(int keyIndex, int curRowIndex) {
		Object value = getValueFromKeyOrCurRow(keyIndex, curRowIndex);
		return value != null ? value.toString() : null;
	}

	/**
	 * Get a boolean by key, falling back to curRow index 0.
	 * 
	 * @param key The key to retrieve from noun store.
	 * @return The boolean value, or null if not found.
	 */
	protected Boolean getBooleanFromKeyOrCurRow(String key) {
		return getBooleanFromKeyOrCurRow(key, 0);
	}

	/**
	 * Get a boolean by key index, falling back to curRow index 0.
	 * 
	 * @param keyIndex The key index in keysToGet.
	 * @return The boolean value, or null if not found.
	 */
	protected Boolean getBooleanFromKeyOrCurRow(int keyIndex) {
		return getBooleanFromKeyOrCurRow(keyIndex, 0);
	}

	/**
	 * Get a boolean by key, falling back to the provided curRow index.
	 * 
	 * @param key         The key to retrieve from noun store.
	 * @param curRowIndex The fallback index in curRow.
	 * @return The boolean value, or null if not found.
	 */
	protected Boolean getBooleanFromKeyOrCurRow(String key, int curRowIndex) {
		Object value = getValueFromKeyOrCurRow(key, curRowIndex);
		if (value == null) {
			return null;
		}
		if (value instanceof Boolean) {
			return (Boolean) value;
		}
		return Boolean.parseBoolean(value.toString());
	}

	/**
	 * Get a boolean by key index, falling back to the provided curRow index.
	 * 
	 * @param keyIndex    The key index in keysToGet.
	 * @param curRowIndex The fallback index in curRow.
	 * @return The boolean value, or null if not found.
	 */
	protected Boolean getBooleanFromKeyOrCurRow(int keyIndex, int curRowIndex) {
		Object value = getValueFromKeyOrCurRow(keyIndex, curRowIndex);
		if (value == null) {
			return null;
		}
		if (value instanceof Boolean) {
			return (Boolean) value;
		}
		return Boolean.parseBoolean(value.toString());
	}

	/**
	 * Get an integer by key, falling back to curRow index 0.
	 * 
	 * @param key The key to retrieve from noun store.
	 * @return The integer value, or null if not found.
	 */
	protected Integer getIntFromKeyOrCurRow(String key) {
		return getIntFromKeyOrCurRow(key, 0);
	}

	/**
	 * Get an integer by key index, falling back to curRow index 0.
	 * 
	 * @param keyIndex The key index in keysToGet.
	 * @return The integer value, or null if not found.
	 */
	protected Integer getIntFromKeyOrCurRow(int keyIndex) {
		return getIntFromKeyOrCurRow(keyIndex, 0);
	}

	/**
	 * Get an integer by key, falling back to the provided curRow index.
	 * 
	 * @param key         The key to retrieve from noun store.
	 * @param curRowIndex The fallback index in curRow.
	 * @return The integer value, or null if not found.
	 */
	protected Integer getIntFromKeyOrCurRow(String key, int curRowIndex) {
		Object value = getValueFromKeyOrCurRow(key, curRowIndex);
		if (value == null) {
			return null;
		}
		if (value instanceof Number) {
			return ((Number) value).intValue();
		}
		return Integer.parseInt(value.toString());
	}

	/**
	 * Get an integer by key index, falling back to the provided curRow index.
	 * 
	 * @param keyIndex    The key index in keysToGet.
	 * @param curRowIndex The fallback index in curRow.
	 * @return The integer value, or null if not found.
	 */
	protected Integer getIntFromKeyOrCurRow(int keyIndex, int curRowIndex) {
		Object value = getValueFromKeyOrCurRow(keyIndex, curRowIndex);
		if (value == null) {
			return null;
		}
		if (value instanceof Number) {
			return ((Number) value).intValue();
		}
		return Integer.parseInt(value.toString());
	}

	/**
	 * Get list values as strings by key, falling back to all curRow values.
	 * 
	 * @param key The key to retrieve from noun store.
	 * @return A list of string values.
	 */
	protected List<String> getListStringFromKeyOrCurRow(String key) {
		return getListStringFromKeyOrCurRow(key, 0);
	}

	/**
	 * Get list string noun values by key, falling back to string noun values from
	 * curRow.
	 * 
	 * @param key The key to retrieve from noun store.
	 * @return A list of string noun values.
	 */
	protected List<String> getListStringNounsFromKeyOrCurRow(String key) {
		GenRowStruct grs = getGenRowStruct(key);
		if (grs != null && !grs.isEmpty()) {
			return grs.getAllStrValues();
		}
		return getCurRowStringValues();
	}

	/**
	 * Get list string noun values by key index, falling back to string noun values
	 * from curRow.
	 * 
	 * @param keyIndex The key index in keysToGet.
	 * @return A list of string noun values.
	 */
	protected List<String> getListStringNounsFromKeyOrCurRow(int keyIndex) {
		GenRowStruct grs = getGenRowStruct(keyIndex);
		if (grs != null && !grs.isEmpty()) {
			return grs.getAllStrValues();
		}
		return getCurRowStringValues();
	}

	/**
	 * Get list values as strings by key index, falling back to all curRow values.
	 * 
	 * @param keyIndex The key index in keysToGet.
	 * @return A list of string values.
	 */
	protected List<String> getListStringFromKeyOrCurRow(int keyIndex) {
		return getListStringFromKeyOrCurRow(keyIndex, 0);
	}

	/**
	 * Get list values as strings by key, falling back to curRow values from a start
	 * index.
	 * 
	 * @param key              The key to retrieve from noun store.
	 * @param curRowStartIndex The fallback start index in curRow.
	 * @return A list of string values.
	 */
	protected List<String> getListStringFromKeyOrCurRow(String key, int curRowStartIndex) {
		GenRowStruct grs = getGenRowStruct(key);
		if (grs != null && !grs.isEmpty()) {
			return convertAllValuesToString(grs);
		}
		return getCurRowValuesAsString(curRowStartIndex);
	}

	/**
	 * Get list values as strings by key index, falling back to curRow values from a
	 * start index.
	 * 
	 * @param keyIndex         The key index in keysToGet.
	 * @param curRowStartIndex The fallback start index in curRow.
	 * @return A list of string values.
	 */
	protected List<String> getListStringFromKeyOrCurRow(int keyIndex, int curRowStartIndex) {
		GenRowStruct grs = getGenRowStruct(keyIndex);
		if (grs != null && !grs.isEmpty()) {
			return convertAllValuesToString(grs);
		}
		return getCurRowValuesAsString(curRowStartIndex);
	}

	/**
	 * Get a string value from the noun store. Assumes the GenRowStruct for the key
	 * contains a single value.
	 * 
	 * @param key The key to retrieve from the noun store.
	 * @return The string value, or null if not found.
	 */
	protected String getString(String key) {
		GenRowStruct grs = getGenRowStruct(key);
		if (grs != null && !grs.isEmpty()) {
			return grs.get(0).toString();
		}
		return null;
	}

	/**
	 * Get a string value from the noun store by index. Assumes the GenRowStruct for
	 * the key contains a single value.
	 * 
	 * @param index The index of the key to retrieve from the noun store.
	 * @return The string value, or null if not found.
	 */
	protected String getString(int index) {
		GenRowStruct grs = getGenRowStruct(index);
		if (grs != null && !grs.isEmpty()) {
			return grs.get(0).toString();
		}
		return null;
	}

	/**
	 * Get a boolean value from the noun store. Assumes the GenRowStruct for the key
	 * contains a single value.
	 * 
	 * @param key The key to retrieve from the noun store.
	 * @return The boolean value, or null if not found.
	 */
	protected Boolean getBoolean(String key) {
		GenRowStruct grs = getGenRowStruct(key);
		if (grs != null && !grs.isEmpty()) {
			Object val = grs.get(0);
			if (val instanceof Boolean) {
				return (Boolean) val;
			}
			return Boolean.parseBoolean(val.toString());
		}
		return null;
	}

	/**
	 * Get a boolean value from the noun store by index. Assumes the GenRowStruct
	 * for the key contains a single value.
	 * 
	 * @param index The index of the key to retrieve from the noun store.
	 * @return The boolean value, or null if not found.
	 */
	protected Boolean getBoolean(int index) {
		GenRowStruct grs = getGenRowStruct(index);
		if (grs != null && !grs.isEmpty()) {
			Object val = grs.get(0);
			if (val instanceof Boolean) {
				return (Boolean) val;
			}
			return Boolean.parseBoolean(val.toString());
		}
		return null;
	}

	/**
	 * Get an integer value from the noun store. Assumes the GenRowStruct for the
	 * key contains a single value.
	 * 
	 * @param key The key to retrieve from the noun store.
	 * @return The integer value, or null if not found.
	 */
	protected Integer getInt(String key) {
		GenRowStruct grs = getGenRowStruct(key);
		if (grs != null && !grs.isEmpty()) {
			Object val = grs.get(0);
			if (val instanceof Number) {
				return ((Number) val).intValue();
			}
			return Integer.parseInt(val.toString());
		}
		return null;
	}

	/**
	 * Get an integer value from the noun store by index. Assumes the GenRowStruct
	 * for the key contains a single value.
	 * 
	 * @param index The index of the key to retrieve from the noun store.
	 * @return The integer value, or null if not found.
	 */
	protected Integer getInt(int index) {
		GenRowStruct grs = getGenRowStruct(index);
		if (grs != null && !grs.isEmpty()) {
			Object val = grs.get(0);
			if (val instanceof Number) {
				return ((Number) val).intValue();
			}
			return Integer.parseInt(val.toString());
		}
		return null;
	}

	/**
	 * Get a double value from the noun store. Assumes the GenRowStruct for the key
	 * contains a single value.
	 * 
	 * @param key The key to retrieve from the noun store.
	 * @return The double value, or null if not found.
	 */
	protected Double getDouble(String key) {
		GenRowStruct grs = getGenRowStruct(key);
		if (grs != null && !grs.isEmpty()) {
			Object val = grs.get(0);
			if (val instanceof Number) {
				return ((Number) val).doubleValue();
			}
			return Double.parseDouble(val.toString());
		}
		return null;
	}

	/**
	 * Get a double value from the noun store by index. Assumes the GenRowStruct for
	 * the key contains a single value.
	 * 
	 * @param index The index of the key to retrieve from the noun store.
	 * @return The double value, or null if not found.
	 */
	protected Double getDouble(int index) {
		GenRowStruct grs = getGenRowStruct(index);
		if (grs != null && !grs.isEmpty()) {
			Object val = grs.get(0);
			if (val instanceof Number) {
				return ((Number) val).doubleValue();
			}
			return Double.parseDouble(val.toString());
		}
		return null;
	}

	/**
	 * Get a long value from the noun store. Assumes the GenRowStruct for the key
	 * contains a single value.
	 * 
	 * @param key The key to retrieve from the noun store.
	 * @return The long value, or null if not found.
	 */
	protected Long getLong(String key) {
		GenRowStruct grs = getGenRowStruct(key);
		if (grs != null && !grs.isEmpty()) {
			Object val = grs.get(0);
			if (val instanceof Number) {
				return ((Number) val).longValue();
			}
			return Long.parseLong(val.toString());
		}
		return null;
	}

	/**
	 * Get a long value from the noun store by index. Assumes the GenRowStruct for
	 * the key contains a single value.
	 * 
	 * @param index The index of the key to retrieve from the noun store.
	 * @return The long value, or null if not found.
	 */
	protected Long getLong(int index) {
		GenRowStruct grs = getGenRowStruct(index);
		if (grs != null && !grs.isEmpty()) {
			Object val = grs.get(0);
			if (val instanceof Number) {
				return ((Number) val).longValue();
			}
			return Long.parseLong(val.toString());
		}
		return null;
	}

	/**
	 * Get a list of objects from the noun store.
	 * 
	 * @param key The key to retrieve from the noun store.
	 * @return A list of objects, or null if not found.
	 */
	@SuppressWarnings("rawtypes")
	protected List getList(String key) {
		GenRowStruct grs = getGenRowStruct(key);
		if (grs != null) {
			return grs.getAllValues();
		}
		return null;
	}

	/**
	 * Get a list of objects from the noun store by index.
	 * 
	 * @param index The index of the key to retrieve from the noun store.
	 * @return A list of objects, or null if not found.
	 */
	@SuppressWarnings("rawtypes")
	protected List getList(int index) {
		GenRowStruct grs = getGenRowStruct(index);
		if (grs != null) {
			return grs.getAllValues();
		}
		return null;
	}

	/**
	 * Get a list of strings from the noun store.
	 * 
	 * @param key The key to retrieve from the noun store.
	 * @return A list of strings, or null if not found.
	 */
	protected List<String> getListString(String key) {
		GenRowStruct grs = getGenRowStruct(key);
		if (grs != null) {
			return grs.getAllStrValues();
		}
		return null;
	}

	/**
	 * Get a list of strings from the noun store by index.
	 * 
	 * @param index The index of the key to retrieve from the noun store.
	 * @return A list of strings, or null if not found.
	 */
	protected List<String> getListString(int index) {
		GenRowStruct grs = getGenRowStruct(index);
		if (grs != null) {
			return grs.getAllStrValues();
		}
		return null;
	}

	/**
	 * Get a map from the noun store. Assumes the GenRowStruct for the key contains
	 * a single map value.
	 * 
	 * @param key The key to retrieve from the noun store.
	 * @return A map, or null if not found.
	 */
	@SuppressWarnings("rawtypes")
	protected Map getMap(String key) {
		GenRowStruct grs = getGenRowStruct(key);
		if (grs != null && !grs.isEmpty()) {
			Object val = grs.get(0);
			if (val instanceof Map) {
				return (Map) val;
			}
		}
		return null;
	}

	/**
	 * Get a map from the noun store by index. Assumes the GenRowStruct for the key
	 * contains a single map value.
	 * 
	 * @param index The index of the key to retrieve from the noun store.
	 * @return A map, or null if not found.
	 */
	@SuppressWarnings("rawtypes")
	protected Map getMap(int index) {
		GenRowStruct grs = getGenRowStruct(index);
		if (grs != null && !grs.isEmpty()) {
			Object val = grs.get(0);
			if (val instanceof Map) {
				return (Map) val;
			}
		}
		return null;
	}

	/**
	 * Get a generic map from the noun store.
	 * 
	 * @param param   The key to retrieve from the noun store.
	 * @param typeRef A TypeReference for the map's generic types.
	 * @return A map, or null if not found.
	 */
	@SuppressWarnings("unchecked")
	protected <K, V> Map<K, V> getGenericMap(String param, TypeReference<Map<K, V>> typeRef) {
		GenRowStruct mapGrs = this.store.getGenRowStruct(param);
		if (mapGrs != null && !mapGrs.isEmpty()) {
			List<NounMetadata> mapInputs = mapGrs.getNounsOfType(PixelDataType.MAP);
			if (mapInputs != null && !mapInputs.isEmpty()) {
				return (Map<K, V>) mapInputs.get(0).getValue();
			}
		}
		List<NounMetadata> mapInputs = this.curRow.getNounsOfType(PixelDataType.MAP);
		if (mapInputs != null && !mapInputs.isEmpty()) {
			return (Map<K, V>) mapInputs.get(0).getValue();
		}
		return null;
	}

	/**
	 * Converts a JSON object string to a Map<String, Object>
	 * 
	 * @param json The JSON string (must be a JSON object: { ... })
	 * @return The parsed Map
	 */
	public Map<String, Object> jsonToMap(String json) {
		if (json == null || json.trim().isEmpty() || !json.trim().startsWith("{")) {
			throw new IllegalArgumentException("Input must be a valid JSON object string.");
		}
		return GSON.fromJson(json, new TypeToken<Map<String, Object>>() {
		}.getType());
	}

	/**
	 * Get a string value from the noun store, with a default value.
	 * 
	 * @param key          The key to retrieve from the noun store.
	 * @param defaultValue The default value to return if the key is not found.
	 * @return The string value, or the default value if not found.
	 */
	protected String getString(String key, String defaultValue) {
		String value = getString(key);
		return value != null ? value : defaultValue;
	}

	/**
	 * Get a string value from the noun store by index, with a default value.
	 * 
	 * @param index        The index of the key to retrieve from the noun store.
	 * @param defaultValue The default value to return if the key is not found.
	 * @return The string value, or the default value if not found.
	 */
	protected String getString(int index, String defaultValue) {
		String value = getString(index);
		return value != null ? value : defaultValue;
	}

	/**
	 * Get a boolean value from the noun store, with a default value.
	 * 
	 * @param key          The key to retrieve from the noun store.
	 * @param defaultValue The default value to return if the key is not found.
	 * @return The boolean value, or the default value if not found.
	 */
	protected Boolean getBoolean(String key, Boolean defaultValue) {
		Boolean value = getBoolean(key);
		return value != null ? value : defaultValue;
	}

	/**
	 * Get a boolean value from the noun store by index, with a default value.
	 * 
	 * @param index        The index of the key to retrieve from the noun store.
	 * @param defaultValue The default value to return if the key is not found.
	 * @return The boolean value, or the default value if not found.
	 */
	protected Boolean getBoolean(int index, Boolean defaultValue) {
		Boolean value = getBoolean(index);
		return value != null ? value : defaultValue;
	}

	/**
	 * Get an integer value from the noun store, with a default value.
	 * 
	 * @param key          The key to retrieve from the noun store.
	 * @param defaultValue The default value to return if the key is not found.
	 * @return The integer value, or the default value if not found.
	 */
	protected Integer getInt(String key, Integer defaultValue) {
		Integer value = getInt(key);
		return value != null ? value : defaultValue;
	}

	/**
	 * Get an integer value from the noun store by index, with a default value.
	 * 
	 * @param index        The index of the key to retrieve from the noun store.
	 * @param defaultValue The default value to return if the key is not found.
	 * @return The integer value, or the default value if not found.
	 */
	protected Integer getInt(int index, Integer defaultValue) {
		Integer value = getInt(index);
		return value != null ? value : defaultValue;
	}

	/**
	 * Get a double value from the noun store, with a default value.
	 * 
	 * @param key          The key to retrieve from the noun store.
	 * @param defaultValue The default value to return if the key is not found.
	 * @return The double value, or the default value if not found.
	 */
	protected Double getDouble(String key, Double defaultValue) {
		Double value = getDouble(key);
		return value != null ? value : defaultValue;
	}

	/**
	 * Get a double value from the noun store by index, with a default value.
	 * 
	 * @param index        The index of the key to retrieve from the noun store.
	 * @param defaultValue The default value to return if the key is not found.
	 * @return The double value, or the default value if not found.
	 */
	protected Double getDouble(int index, Double defaultValue) {
		Double value = getDouble(index);
		return value != null ? value : defaultValue;
	}

	/**
	 * Get a long value from the noun store, with a default value.
	 * 
	 * @param key          The key to retrieve from the noun store.
	 * @param defaultValue The default value to return if the key is not found.
	 * @return The long value, or the default value if not found.
	 */
	protected Long getLong(String key, Long defaultValue) {
		Long value = getLong(key);
		return value != null ? value : defaultValue;
	}

	/**
	 * Get a long value from the noun store by index, with a default value.
	 * 
	 * @param index        The index of the key to retrieve from the noun store.
	 * @param defaultValue The default value to return if the key is not found.
	 * @return The long value, or the default value if not found.
	 */
	protected Long getLong(int index, Long defaultValue) {
		Long value = getLong(index);
		return value != null ? value : defaultValue;
	}

	/**
	 * Get a list of objects from the noun store, with a default value.
	 * 
	 * @param key          The key to retrieve from the noun store.
	 * @param defaultValue The default value to return if the key is not found.
	 * @return A list of objects, or the default value if not found.
	 */
	@SuppressWarnings("rawtypes")
	protected List getList(String key, List defaultValue) {
		List value = getList(key);
		return value != null ? value : defaultValue;
	}

	/**
	 * Get a list of objects from the noun store by index, with a default value.
	 * 
	 * @param index        The index of the key to retrieve from the noun store.
	 * @param defaultValue The default value to return if the key is not found.
	 * @return A list of objects, or the default value if not found.
	 */
	@SuppressWarnings("rawtypes")
	protected List getList(int index, List defaultValue) {
		List value = getList(index);
		return value != null ? value : defaultValue;
	}

	/**
	 * Get a list of strings from the noun store, with a default value.
	 * 
	 * @param key          The key to retrieve from the noun store.
	 * @param defaultValue The default value to return if the key is not found.
	 * @return A list of strings, or the default value if not found.
	 */
	protected List<String> getListString(String key, List<String> defaultValue) {
		List<String> value = getListString(key);
		return value != null ? value : defaultValue;
	}

	/**
	 * Get a list of strings from the noun store by index, with a default value.
	 * 
	 * @param index        The index of the key to retrieve from the noun store.
	 * @param defaultValue The default value to return if the key is not found.
	 * @return A list of strings, or the default value if not found.
	 */
	protected List<String> getListString(int index, List<String> defaultValue) {
		List<String> value = getListString(index);
		return value != null ? value : defaultValue;
	}

	/**
	 * Get a map from the noun store, with a default value.
	 * 
	 * @param key          The key to retrieve from the noun store.
	 * @param defaultValue The default value to return if the key is not found.
	 * @return A map, or the default value if not found.
	 */
	@SuppressWarnings("rawtypes")
	protected Map getMap(String key, Map defaultValue) {
		Map value = getMap(key);
		return value != null ? value : defaultValue;
	}

	/**
	 * Get a map from the noun store by index, with a default value.
	 * 
	 * @param index        The index of the key to retrieve from the noun store.
	 * @param defaultValue The default value to return if the key is not found.
	 * @return A map, or the default value if not found.
	 */
	@SuppressWarnings("rawtypes")
	protected Map getMap(int index, Map defaultValue) {
		Map value = getMap(index);
		return value != null ? value : defaultValue;
	}

	/**
	 * Get a list of integers from the noun store.
	 * 
	 * @param key The key to retrieve from the noun store.
	 * @return A list of integers, or null if not found.
	 */
	protected List<Integer> getListInteger(String key) {
		GenRowStruct grs = getGenRowStruct(key);
		if (grs != null) {
			return grs.getAllNumericColumnsAsInteger();
		}
		return null;
	}

	/**
	 * Get a list of integers from the noun store by index.
	 * 
	 * @param index The index of the key to retrieve from the noun store.
	 * @return A list of integers, or null if not found.
	 */
	protected List<Integer> getListInteger(int index) {
		GenRowStruct grs = getGenRowStruct(index);
		if (grs != null) {
			return grs.getAllNumericColumnsAsInteger();
		}
		return null;
	}

	/**
	 * Get a list of integers from the noun store, with a default value.
	 * 
	 * @param key          The key to retrieve from the noun store.
	 * @param defaultValue The default value to return if the key is not found.
	 * @return A list of integers, or the default value if not found.
	 */
	protected List<Integer> getListInteger(String key, List<Integer> defaultValue) {
		List<Integer> value = getListInteger(key);
		return value != null ? value : defaultValue;
	}

	/**
	 * Get a list of integers from the noun store by index, with a default value.
	 * 
	 * @param index        The index of the key to retrieve from the noun store.
	 * @param defaultValue The default value to return if the key is not found.
	 * @return A list of integers, or the default value if not found.
	 */
	protected List<Integer> getListInteger(int index, List<Integer> defaultValue) {
		List<Integer> value = getListInteger(index);
		return value != null ? value : defaultValue;
	}

	/**
	 * Get a list of doubles from the noun store.
	 * 
	 * @param key The key to retrieve from the noun store.
	 * @return A list of doubles, or null if not found.
	 */
	protected List<Double> getListDouble(String key) {
		GenRowStruct grs = getGenRowStruct(key);
		if (grs != null) {
			return grs.getAllNumericColumnsAsDouble();
		}
		return null;
	}

	/**
	 * Get a list of doubles from the noun store by index.
	 * 
	 * @param index The index of the key to retrieve from the noun store.
	 * @return A list of doubles, or null if not found.
	 */
	protected List<Double> getListDouble(int index) {
		GenRowStruct grs = getGenRowStruct(index);
		if (grs != null) {
			return grs.getAllNumericColumnsAsDouble();
		}
		return null;
	}

	/**
	 * Get a list of doubles from the noun store, with a default value.
	 * 
	 * @param key          The key to retrieve from the noun store.
	 * @param defaultValue The default value to return if the key is not found.
	 * @return A list of doubles, or the default value if not found.
	 */
	protected List<Double> getListDouble(String key, List<Double> defaultValue) {
		List<Double> value = getListDouble(key);
		return value != null ? value : defaultValue;
	}

	/**
	 * Get a list of doubles from the noun store by index, with a default value.
	 * 
	 * @param index        The index of the key to retrieve from the noun store.
	 * @param defaultValue The default value to return if the key is not found.
	 * @return A list of doubles, or the default value if not found.
	 */
	protected List<Double> getListDouble(int index, List<Double> defaultValue) {
		List<Double> value = getListDouble(index);
		return value != null ? value : defaultValue;
	}

	/**
	 * 
	 * @param input
	 * @return
	 */
	public String fillVars(String input) {
		// ${i} - insight id
		// ${iid} - insight id
		// ${insight_id} - insight id
		// ${if} - insight folder
		// ${i_f} - Insight folder
		// ${p} - project folder assets
		// ${project} - project
		// ${p_id} - project id
		// ${pid} - project id

		String insightId = this.insight.getInsightId();
		String insightFolder = this.insight.getInsightFolder()
				.replace(java.nio.file.FileSystems.getDefault().getSeparator(), "/");
		String projectId = this.insight.getProjectId();
		String projectFolder = this.insight.getAppFolder();
		if (projectId == null) {
			projectId = insightId;
		}

		if (projectFolder == null) {
			projectFolder = insightFolder;
		} else {
			projectFolder = projectFolder.replace(java.nio.file.FileSystems.getDefault().getSeparator(), "/");
		}

		Map<String, String> varMap = new HashMap<String, String>();
		varMap.put("i", insightId);
		varMap.put("iid", insightId);
		varMap.put("i_id", insightId);
		varMap.put("insight_id", insightId);
		varMap.put("if", insightFolder);
		varMap.put("i_f", insightFolder);
		varMap.put("p", projectId);
		varMap.put("pid", projectId);
		varMap.put("pf", projectFolder);
		varMap.put("p_f", projectFolder);

		StringSubstitutor sub = new StringSubstitutor(varMap);
		String resolvedString = sub.replace(input);
		return resolvedString;
	}

}
