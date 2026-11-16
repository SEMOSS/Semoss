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
package prerna.sablecc2.pipeline;

import java.io.File;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import prerna.algorithm.api.ITableDataFrame;
import prerna.om.Insight;
import prerna.om.Pixel;
import prerna.poi.main.helper.CSVFileHelper;
import prerna.query.querystruct.AbstractQueryStruct;
import prerna.reactor.IReactor;
import prerna.reactor.qs.AbstractQueryStructReactor;
import prerna.sablecc2.LazyTranslation;
import prerna.sablecc2.PixelUtility;
import prerna.sablecc2.node.ABooleanScalar;
import prerna.sablecc2.node.AFractionDecimal;
import prerna.sablecc2.node.AIdWordOrId;
import prerna.sablecc2.node.ARoutineConfiguration;
import prerna.sablecc2.node.AWholeDecimal;
import prerna.sablecc2.node.AWordWordOrId;
import prerna.sablecc2.node.PRoutine;
import prerna.sablecc2.om.GenRowStruct;
import prerna.sablecc2.om.NounStore;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.VarStore;
import prerna.sablecc2.om.execptions.SemossPixelException;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.Utility;

public class PipelineTranslation extends LazyTranslation {

	private static final Logger classLogger = LogManager.getLogger(PipelineTranslation.class);

	private static Map<String, String> reactorToId = null;

	private static Map<AbstractQueryStruct.QUERY_STRUCT_TYPE, String> qsToWidget = new HashMap<>();
	private static List<String> qsReactors = new ArrayList<>();
	private static List<String> fileReactors = new ArrayList<>();
	private static List<String> codeBlocks = new ArrayList<>();
	static {
		qsToWidget.put(AbstractQueryStruct.QUERY_STRUCT_TYPE.CSV_FILE, "pipeline-file");
		qsToWidget.put(AbstractQueryStruct.QUERY_STRUCT_TYPE.ENGINE, "pipeline-app");
		qsToWidget.put(AbstractQueryStruct.QUERY_STRUCT_TYPE.EXCEL_FILE, "pipeline-file");
		qsToWidget.put(AbstractQueryStruct.QUERY_STRUCT_TYPE.FRAME, "pipeline-frame");
		qsToWidget.put(AbstractQueryStruct.QUERY_STRUCT_TYPE.RAW_ENGINE_QUERY, "pipeline-query");
		qsToWidget.put(AbstractQueryStruct.QUERY_STRUCT_TYPE.RAW_JDBC_ENGINE_QUERY, "pipeline-external");
		qsToWidget.put(AbstractQueryStruct.QUERY_STRUCT_TYPE.RAW_RDF_FILE_ENGINE_QUERY, "pipeline-rdf-file");

		// frame importing
		qsReactors.add("Import");
		qsReactors.add("Merge");
		qsReactors.add("FuzzyMerge");
		qsReactors.add("Union");

		// file + database importing
		fileReactors.add("ToCsv");
		fileReactors.add("ToTsv");
		fileReactors.add("ToTxt");
		fileReactors.add("ToExcel");
		fileReactors.add("ToDatabase");

		codeBlocks.add("R");
		codeBlocks.add("Py");
		codeBlocks.add("Java");
	}

	private List<List<PipelineOperation>> allRoutines = new ArrayList<>();
	private List<PipelineOperation> curRoutine;
	private List<Map<String, Object>> pixelIdToOperation = new ArrayList<>();

	public PipelineTranslation(Insight insight, Map<String, String> encodedTextToOriginal) {
		super();
		this.insight = insight;
		this.encodedTextToOriginal = encodedTextToOriginal;
		if (this.insight != null) {
			// we will copy the var store
			VarStore copy = new VarStore();
			copy.putAll(insight.getVarStore());
			this.planner.setVarStore(copy);
		}
		if (reactorToId == null) {
			init();
		}
	}

	@Override
	public void caseARoutineConfiguration(ARoutineConfiguration node) {
		List<PRoutine> copy = new ArrayList<>(node.getRoutine());
		int size = copy.size();
		for (int pixelstep = 0; pixelstep < size; pixelstep++) {
			long start = System.currentTimeMillis();
			PRoutine e = copy.get(pixelstep);
			try {
				this.resultKey = "$RESULT_" + e.hashCode();
				this.pixelObj = new Pixel("tempStorage", e.toString());
				this.pixelObj.startTime();

				this.curRoutine = new ArrayList<>();
				// add the routine
				this.allRoutines.add(this.curRoutine);

				e.apply(this);
				// reset the state of the frame
				this.currentFrame = null;
			} catch (SemossPixelException ex) {
				trackError(e.toString(), this.pixelObj.isMeta(), ex);
				classLogger.error("Pixel error while building the pipeline for step {} of {}: {}", pixelstep + 1, size,
						e.toString(), ex);
				// if we want to continue the thread of execution
				// nothing special
				// just add the error to the return
				if (ex.isContinueThreadOfExecution()) {
					planner.addVariable("$RESULT", ex.getNoun());
					postProcess(e.toString().trim());
				} else {
					// if we do want to stop
					// propagate the error up and the PixelRunner
					// will handle grabbing the meta and returning it to the FE
					postRuntimeErrorProcess(e.toString(), ex.getNoun(), copy.subList(pixelstep + 1, size).stream()
							.map(p -> p.toString()).collect(Collectors.toList()));
					throw ex;
				}
			} catch (Exception ex) {
				trackError(e.toString(), this.pixelObj.isMeta(), ex);
				classLogger.error("Unexpected error while building the pipeline for step {} of {}: {}", pixelstep + 1,
						size, e.toString(), ex);
				planner.addVariable("$RESULT",
						new NounMetadata(ex.getMessage(), PixelDataType.ERROR, PixelOperationType.ERROR));
				postProcess(e.toString().trim());
			}
			long end = System.currentTimeMillis();
			classLogger.debug("Time to process = {} ms for {}", (end - start), e.toString());
		}
	}

	////////////////////////////////////////////////////////////////////////////////

	/*
	 * Scalar values
	 */

	@Override
	public void inAIdWordOrId(AIdWordOrId node) {
		super.inAIdWordOrId(node);
		if (this.curReactor == null) {
			ConstantPipelineOperation op = new ConstantPipelineOperation("direct value", node.toString());

			String idInput = node.getId().getText().trim();

			Map<String, Object> obj = new HashMap<>();
			if (this.planner.hasVariable(idInput)) {
				NounMetadata noun = this.planner.getVariable(idInput);
				if (noun.getNounType() != PixelDataType.LAMBDA) {
					if (noun.getNounType() == PixelDataType.FRAME) {
						op = new ConstantPipelineOperation(idInput, idInput);
						obj.putAll(processFrameNounMap(noun));
					} else {
						op = new ConstantPipelineOperation(idInput, idInput);
						obj.putAll(processBasicNounMap(noun));
					}
				}
			}
			// if we haven't filled it
			// just set to default
			if (obj.isEmpty()) {
				op = new ConstantPipelineOperation("direct value", idInput);
				obj.put("type", "COLUMN_OR_VAR");
				obj.put("value", idInput);
			}

			op.setScalarMap(obj);
			this.curRoutine.add(op);
		}
	}

	@Override
	public void inAWordWordOrId(AWordWordOrId node) {
		super.inAWordWordOrId(node);
		if (this.curReactor == null) {
			Map<String, Object> obj = new HashMap<>();
			obj.put("type", "STRING");
			obj.put("value", PixelUtility.decodePixelStringLiteral(node.getWord().getText()));

			ConstantPipelineOperation op = new ConstantPipelineOperation("direct value", node.toString());
			op.setScalarMap(obj);
			this.curRoutine.add(op);
		}
	}

	@Override
	public void inAFractionDecimal(AFractionDecimal node) {
		super.inAFractionDecimal(node);
		if (this.curReactor == null) {
			Map<String, Object> obj = new HashMap<>();
			obj.put("type", "NUMBER");
			String fraction = (node.getFraction() + "").trim();
			Number retNum = new BigDecimal("0." + fraction);
			obj.put("value", retNum);

			ConstantPipelineOperation op = new ConstantPipelineOperation("direct value", node.toString());
			op.setScalarMap(obj);
			this.curRoutine.add(op);
		}
	}

	@Override
	public void inAWholeDecimal(AWholeDecimal node) {
		super.inAWholeDecimal(node);
		if (this.curReactor == null) {
			Map<String, Object> obj = new HashMap<>();
			obj.put("type", "NUMBER");

			boolean isDouble = false;
			String whole = "";
			String fraction = "";
			// get the whole portion
			if (node.getWhole() != null) {
				whole = node.getWhole().toString().trim();
			}
			// get the fraction portion
			if (node.getFraction() != null) {
				isDouble = true;
				fraction = (node.getFraction() + "").trim();
			} else {
				fraction = "0";
			}
			Number retNum = new BigDecimal(whole + "." + fraction);

			if (isDouble) {
				obj.put("value", retNum.doubleValue());
			} else {
				obj.put("value", retNum.intValue());
			}

			ConstantPipelineOperation op = new ConstantPipelineOperation("direct value", node.toString());
			op.setScalarMap(obj);
			this.curRoutine.add(op);
		}
	}

	@Override
	public void inABooleanScalar(ABooleanScalar node) {
		super.inABooleanScalar(node);
		if (this.curReactor == null) {
			String booleanStr = node.getBoolean().toString().trim();
			Boolean bool = Boolean.parseBoolean(booleanStr);
			Map<String, Object> obj = new HashMap<>();
			obj.put("type", "BOOLEAN");
			obj.put("value", bool);
			ConstantPipelineOperation op = new ConstantPipelineOperation("direct value", node.toString());
			op.setScalarMap(obj);
			this.curRoutine.add(op);
		}
	}

	////////////////////////////////////////////////////////////////////////////////

	/**
	 * Same method as in lazy with addition of addRoutine method
	 */
	@Override
	protected void deInitReactor() {
		if (curReactor != null) {
			// merge up and update the plan
			try {
				curReactor.mergeUp();
				curReactor.updatePlan();
				addRoutine();
			} catch (Exception e) {
				classLogger.error("Unable to generate the pipeline operation for reactor {}",
						curReactor.getClass().getSimpleName(), e);
				throw new IllegalArgumentException(e.getMessage(), e);
			}

			// get the parent
			Object parent = curReactor.Out();
			// set the parent as the curReactor if it is present
			prevReactor = curReactor;
			if (parent instanceof IReactor) {
				curReactor = (IReactor) parent;
			} else {
				curReactor = null;
			}

			// account for moving qs
			if (curReactor == null && prevReactor instanceof AbstractQueryStructReactor) {
				AbstractQueryStruct qs = ((AbstractQueryStructReactor) prevReactor).getQs();
				this.planner.addVariable(this.resultKey, new NounMetadata(qs, PixelDataType.QUERY_STRUCT));
			}

		}
	}

	private void addRoutine() {
		if (this.curReactor.getParentReactor() == null) {
			// we have a reactor with no parent
			// let us go and process the operation
			// this method is recursive to handle embedded reactors
			// which would be children for this root reactor
			PipelineOperation op = generatePipelineOperation(this.curReactor);

			// add the op to the routine
			this.curRoutine.add(op);
		}
	}

	////////////////////////////////////////////////////////////////////////////////

	private PipelineOperation generatePipelineOperation(IReactor reactor) {
		String[] reactorInputs = reactor.getPixel();
		String reactorId = reactorInputs[0];
		PipelineOperation op = new PipelineOperation(reactorId, reactorInputs[1]);

		// the file reactors do not set the widget id directly since
		// the structure doesn't care about the qs source at the moment...
		// so will set it here if reactor to id has the stuff
		// the import/merge are not defined here
		if (PipelineTranslation.reactorToId != null && PipelineTranslation.reactorToId.containsKey(reactorId)) {
			op.setWidgetId(PipelineTranslation.reactorToId.get(reactorId));
		}

		if (PipelineTranslation.codeBlocks.contains(reactorId)) {
			// we need to process codeblock a bit different
			// we want to decode what we input
			NounStore store = reactor.getNounStore();
			GenRowStruct struct = store.getGenRowStruct("all");
			if (struct != null && !struct.isEmpty()) {
				String value = struct.get(0).toString();
				value = Utility.decodeURIComponent(value);
				NounMetadata noun = new NounMetadata(value, PixelDataType.CONST_STRING);
				Map<String, Object> nounMapRet = processNounMetadata(noun);
				op.addRowInput(nounMapRet);
			}
		} else {
			Map<String, List<Map>> storeMap = reactor.getStoreMap();
			op.setNounInputs(storeMap);
		}

		return op;
	}

	////////////////////////////////////////////////////////////////////////////////////////
	////////////////////////////////////////////////////////////////////////////////////////
	////////////////////////////////////////////////////////////////////////////////////////

	/*
	 * Process noun metadata based on the type + allow for gson
	 */

	private Map<String, Object> processNounMetadata(NounMetadata noun) {
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
		lambdaMap.put("value", generatePipelineOperation((IReactor) noun.getValue()));
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

	////////////////////////////////////////////////////////////////////////////////////////
	////////////////////////////////////////////////////////////////////////////////////////
	////////////////////////////////////////////////////////////////////////////////////////

	/**
	 * Return the reactor based on the reactorId
	 * 
	 * @param reactorId  - reactor id or operation
	 * @param nodeString - full operation
	 * @return IReactor A new instance of the reactor
	 */
	@Override
	protected IReactor getReactor(String reactorId, String nodeString) {
		try {
			return super.getReactor(reactorId, nodeString);
		} catch (Exception e) {
			// error finding reactor
			// just return a generic reactor placeholder
			classLogger.warn("Unable to find reactor {}, falling back to a placeholder pipeline operation", reactorId,
					e);
		}

		UndeterminedPipelineReactor reactor = new UndeterminedPipelineReactor();
		reactor.setPixel(reactorId, nodeString);
		return reactor;
	}

	private static synchronized void init() {
		if (PipelineTranslation.reactorToId == null) {
			String baseFolder = Utility.getBaseFolder();
			File reactorWidgetFile = new File(baseFolder + "/reactorToWidget.csv");
			if (reactorWidgetFile.exists()) {
				CSVFileHelper helper = new CSVFileHelper();
				helper.parse(reactorWidgetFile.getAbsolutePath());

				PipelineTranslation.reactorToId = new HashMap<>();

				String[] row = null;
				while ((row = helper.getNextRow()) != null) {
					PipelineTranslation.reactorToId.put(row[0], row[1]);
				}
			}
		}
	}

	public List<List<PipelineOperation>> getAllRoutines() {
		return this.allRoutines;
	}

	public List<Map<String, Object>> getPixelIdToOperation() {
		return pixelIdToOperation;
	}
}
