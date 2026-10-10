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
package prerna.reactor.automation.run;

import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import prerna.algorithm.api.DataFrameTypeEnum;
import prerna.algorithm.api.ITableDataFrame;
import prerna.ds.nativeframe.NativeFrame;
import prerna.ds.py.PyTranslator;
import prerna.om.Insight;
import prerna.reactor.automation.utils.AutomationRuntimeUtils;
import prerna.reactor.frame.convert.ConvertReactor;
import prerna.reactor.frame.py.GenerateFrameFromPyVariableReactor;
import prerna.sablecc2.om.NounStore;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;

/**
 * Owns the boundary between an Automation node result and an Insight-owned
 * SEMOSS frame.
 *
 * <p>
 * Query execution and Python execution remain with their existing owners. This
 * class only performs the shared frame concerns: registering the frame under the
 * node output alias, deriving a bounded summary from the registered frame,
 * resolving a live backend for downstream scope, and cleaning a failed Python
 * value. The execution Insight remains the sole owner of frame lifetime.
 */
final class AutomationFrameOutput {

	private static final Logger classLogger = LogManager.getLogger(AutomationFrameOutput.class);

	private AutomationFrameOutput() {
	}

	/** Registers an existing frame and returns its backend identity and summary. */
	static RegisteredFrame register(Insight insight, String outputVariable, ITableDataFrame frame) {
		return register(insight, outputVariable, frame, frame.size(outputVariable));
	}

	/** Registers an existing frame with a row count resolved by its query owner. */
	static RegisteredFrame register(Insight insight, String outputVariable, ITableDataFrame frame, long rowCount) {
		if (insight == null || frame == null || outputVariable == null || outputVariable.isBlank()) {
			throw new IllegalArgumentException("Automation frame registration requires an Insight, frame, and alias.");
		}
		if (rowCount < 0) {
			throw new IllegalArgumentException("Automation frame registration requires a nonnegative row count.");
		}
		RegisteredFrame output = describe(frame, rowCount);
		NounMetadata noun = new NounMetadata(frame, PixelDataType.FRAME,
				PixelOperationType.FRAME_DATA_CHANGE, PixelOperationType.FRAME_HEADERS_CHANGE);
		insight.getVarStore().put(outputVariable, noun);
		return output;
	}

	/**
	 * Converts lazy native bindings only when a Python node needs them. Java-owned
	 * consumers, including loops and UI paging, continue to query the native frame
	 * without materializing the complete result.
	 */
	static void materializePythonBindings(Insight insight, Map<String, String> frameBindings,
			Set<String> requiredBindings, String runId, String nodeId) {
		for (Map.Entry<String, String> binding : frameBindings.entrySet()) {
			if (!requiredBindings.contains(binding.getKey())
					|| !DataFrameTypeEnum.NATIVE.getTypeAsString().equals(binding.getValue())) {
				continue;
			}
			String alias = binding.getKey();
			NounMetadata noun = insight.getVarStore().get(alias);
			if (noun == null || !(noun.getValue() instanceof NativeFrame nativeFrame)) {
				throw new IllegalStateException("Automation native frame output '" + alias
						+ "' is no longer available in execution Insight '" + insight.getInsightId() + "'.");
			}

			NounStore nounStore = new NounStore("Convert");
			nounStore.makeGenRowStruct(ReactorKeysEnum.FRAME.getKey())
					.add(new NounMetadata(nativeFrame, PixelDataType.FRAME));
			nounStore.makeGenRowStruct(ReactorKeysEnum.FRAME_TYPE.getKey())
					.add(new NounMetadata(DataFrameTypeEnum.PYTHON.getTypeAsString(), PixelDataType.CONST_STRING));
			nounStore.makeGenRowStruct(ReactorKeysEnum.ALIAS.getKey())
					.add(new NounMetadata(alias, PixelDataType.ALIAS));

			ConvertReactor reactor = new ConvertReactor();
			reactor.setInsight(insight);
			reactor.setNounStore(nounStore);
			NounMetadata converted = reactor.execute();
			if (!(converted.getValue() instanceof ITableDataFrame pythonFrame)
					|| pythonFrame.getFrameType() != DataFrameTypeEnum.PYTHON) {
				throw new IllegalStateException("SEMOSS did not convert native frame '" + alias + "' to Python.");
			}

			binding.setValue(DataFrameTypeEnum.PYTHON.getTypeAsString());
			try {
				nativeFrame.close();
			} catch (RuntimeException cleanupError) {
				classLogger.warn("Unable to close native frame '{}' after materializing Automation run '{}', node '{}'",
						alias, runId, nodeId, cleanupError);
			}
			insight.getVarStore().getAllCreatedFrames().remove(nativeFrame);
		}
	}

	/**
	 * Registers a Python variable through the same bridge used by Notebook and
	 * returns the registered frame's actual shape and backend.
	 */
	static RegisteredFrame registerPythonVariable(Insight insight, String outputVariable) {
		if (insight == null || outputVariable == null || outputVariable.isBlank()) {
			throw new IllegalArgumentException("Automation Python frame registration requires an Insight and alias.");
		}
		NounStore nounStore = new NounStore("GenerateFrameFromPyVariable");
		nounStore.makeGenRowStruct(ReactorKeysEnum.VARIABLE.getKey())
				.add(new NounMetadata(outputVariable, PixelDataType.CONST_STRING));
		nounStore.makeGenRowStruct(ReactorKeysEnum.OVERRIDE.getKey())
				.add(new NounMetadata(false, PixelDataType.BOOLEAN));

		GenerateFrameFromPyVariableReactor reactor = new GenerateFrameFromPyVariableReactor();
		reactor.setInsight(insight);
		reactor.setNounStore(nounStore);
		ITableDataFrame frame = null;
		try {
			NounMetadata result = reactor.execute();
			if (!(result.getValue() instanceof ITableDataFrame registeredFrame)) {
				throw new IllegalStateException("SEMOSS frame registration returned a non-frame value.");
			}
			frame = registeredFrame;
			return describe(frame, frame.size(outputVariable));
		} catch (RuntimeException e) {
			if (frame != null) {
				removeFailedRegistration(insight, outputVariable, frame);
			}
			throw new IllegalStateException(
					"Unable to register Python frame output '" + outputVariable + "' in execution Insight '"
							+ insight.getInsightId() + "'.",
					e);
		}
	}

	private static void removeFailedRegistration(Insight insight, String outputVariable, ITableDataFrame frame) {
		NounMetadata registered = insight.getVarStore().get(outputVariable);
		if (registered != null && registered.getValue() == frame) {
			insight.getVarStore().remove(outputVariable);
		}
		try {
			frame.close();
			insight.getVarStore().getAllCreatedFrames().remove(frame);
		} catch (RuntimeException cleanupError) {
			classLogger.warn("Unable to close failed Python frame output '{}' in execution Insight '{}'",
					outputVariable, insight.getInsightId(), cleanupError);
		}
	}

	/** Returns the backend for a frame registered under an output alias. */
	static String requireBackend(Insight insight, String outputVariable) {
		NounMetadata noun = insight.getVarStore().get(outputVariable);
		if (noun == null || noun.getNounType() != PixelDataType.FRAME
				|| !(noun.getValue() instanceof ITableDataFrame frame)) {
			throw new IllegalStateException("Automation frame output '" + outputVariable
					+ "' was not registered in execution Insight '" + insight.getInsightId() + "'.");
		}
		return frame.getFrameType().getTypeAsString();
	}

	/** Removes a failed or unsupported frame value from the Insight Python session. */
	static void discardPythonValue(PyTranslator translator, String outputVariable, String runId, String nodeId) {
		try {
			translator.runScript("globals().pop(" + AutomationRuntimeUtils.GSON.toJson(outputVariable) + ", None)");
		} catch (RuntimeException cleanupError) {
			classLogger.warn("Unable to discard Python frame output '{}' for Automation run '{}', node '{}'",
					outputVariable, runId, nodeId, cleanupError);
		}
	}

	/**
	 * Releases frames created by one loop iteration without touching frames borrowed
	 * from the parent scope. Frame identity, rather than a reusable output alias,
	 * determines ownership.
	 */
	static void releaseIterationFrames(Insight insight, PyTranslator translator, Set<ITableDataFrame> ownedFrames,
			Map<String, String> frameBindings, String runId, String loopNodeId, int iteration) {
		for (ITableDataFrame frame : ownedFrames) {
			Set<String> aliases = new LinkedHashSet<>(insight.getVarStore().findAllVarReferencesForFrame(frame));
			for (String alias : aliases) {
				insight.getVarStore().remove(alias);
				frameBindings.remove(alias);
			}
			try {
				if (!frame.isClosed()) {
					frame.close();
				}
			} catch (RuntimeException cleanupError) {
				classLogger.warn(
						"Unable to close iteration frame for Automation run '{}', loop '{}', iteration {}",
						runId, loopNodeId, iteration, cleanupError);
			} finally {
				insight.getVarStore().getAllCreatedFrames().remove(frame);
				for (String alias : aliases) {
					discardPythonValue(translator, alias, runId, loopNodeId);
				}
			}
		}
	}

	private static RegisteredFrame describe(ITableDataFrame frame, long rowCount) {
		int columnCount = frame.getColumnHeaders().length;
		Map<String, Object> summary = new LinkedHashMap<>();
		summary.put("dataType", "table");
		summary.put("rowCount", rowCount);
		summary.put("columnCount", columnCount);
		return new RegisteredFrame(summary, frame.getFrameType().getTypeAsString(), frame);
	}

	record RegisteredFrame(Map<String, Object> summary, String backend, ITableDataFrame frame) {
	}
}
