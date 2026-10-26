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

import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import prerna.algorithm.api.ITableDataFrame;
import prerna.engine.api.IHeadersDataRow;
import prerna.engine.api.IRawSelectWrapper;
import prerna.om.Insight;
import prerna.query.querystruct.SelectQueryStruct;
import prerna.query.querystruct.selectors.QueryColumnSelector;
import prerna.reactor.automation.utils.AutomationRuntimeUtils;
import prerna.reactor.frame.py.GenerateFrameFromPyVariableReactor;
import prerna.sablecc2.om.NounStore;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;

/**
 * Owns Automation's reference to an Insight-owned SEMOSS frame.
 *
 * <p>
 * The frame remains the live node-to-node value. Java carries this bounded
 * reference, Python binds the existing frame variable, and consumers page only
 * the rows they need. Full table rows are never copied into Automation scope.
 */
final class AutomationFrameOutput {

	private static final Logger classLogger = LogManager.getLogger(AutomationFrameOutput.class);

	private AutomationFrameOutput() {
	}

	/** Registers an existing frame under the node output alias. */
	static RegisteredFrame register(Insight insight, String outputVariable, ITableDataFrame frame) {
		if (insight == null || frame == null || outputVariable == null || outputVariable.isBlank()) {
			throw new IllegalArgumentException("Automation frame registration requires an Insight, frame, and alias.");
		}
		RegisteredFrame output = describe(outputVariable, frame);
		NounMetadata noun = new NounMetadata(frame, PixelDataType.FRAME,
				PixelOperationType.FRAME_DATA_CHANGE, PixelOperationType.FRAME_HEADERS_CHANGE);
		insight.getVarStore().put(outputVariable, noun);
		return output;
	}

	/** Registers a returned pandas variable through the standard SEMOSS bridge. */
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
			return describe(outputVariable, frame);
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

	private static RegisteredFrame describe(String outputVariable, ITableDataFrame frame) {
		long rowCount = frame.size(outputVariable);
		String[] selectors = frame.getQsHeaders();
		Map<String, Object> summary = new LinkedHashMap<>();
		summary.put("dataType", "table");
		summary.put("rowCount", rowCount);
		summary.put("columnCount", selectors.length);
		return new RegisteredFrame(outputVariable, frame, Map.copyOf(summary),
				frame.getFrameType().getTypeAsString());
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

	/** Live, internal frame reference carried by Automation scope. */
	record RegisteredFrame(String outputVariable, ITableDataFrame frame, Map<String, Object> summary, String backend) {

		long rowCount() {
			return ((Number) summary.get("rowCount")).longValue();
		}

		List<Map<String, Object>> columns() {
			String[] selectors = frame.getQsHeaders();
			String[] headers = frame.getColumnHeaders();
			List<Map<String, Object>> columns = new ArrayList<>(selectors.length);
			for (int index = 0; index < selectors.length; index++) {
				Map<String, Object> column = new LinkedHashMap<>();
				column.put("name", index < headers.length ? headers[index] : selectors[index]);
				column.put("type", frame.getMetaData().getHeaderTypeAsString(selectors[index]));
				columns.add(column);
			}
			return columns;
		}

		List<Object> page(long offset, int limit) {
			if (offset < 0 || limit < 0 || offset > rowCount() || limit > rowCount() - offset) {
				throw new IllegalArgumentException("Frame page is outside the live row range.");
			}
			if (limit == 0) {
				return List.of();
			}
			SelectQueryStruct query = new SelectQueryStruct();
			for (String selector : frame.getQsHeaders()) {
				query.addSelector(new QueryColumnSelector(selector));
			}
			query.setDistinct(false);
			query.setOffSet(offset);
			query.setLimit(limit);
			String[] selectors = frame.getQsHeaders();
			String[] visibleHeaders = frame.getColumnHeaders();
			List<Object> rows = new ArrayList<>(limit);
			try (IRawSelectWrapper wrapper = frame.query(query)) {
				while (wrapper.hasNext()) {
					IHeadersDataRow row = wrapper.next();
					Object[] values = row.getValues();
					if (values.length != selectors.length) {
						throw new IllegalStateException(
								"Automation frame '" + outputVariable + "' returned an inconsistent row schema.");
					}
					Map<String, Object> item = new LinkedHashMap<>();
					for (int index = 0; index < selectors.length; index++) {
						String header = visibleHeaders != null && index < visibleHeaders.length
								? visibleHeaders[index]
								: selectors[index];
						item.put(header, values[index]);
					}
					rows.add(item);
				}
			} catch (IOException e) {
				classLogger.error("Unable to close Automation frame '{}' after reading rows", outputVariable, e);
				throw new IllegalStateException("Unable to finish reading Automation frame '" + outputVariable + "'.", e);
			} catch (RuntimeException e) {
				throw e;
			} catch (Exception e) {
				classLogger.error("Unable to read Automation frame '{}'", outputVariable, e);
				throw new IllegalStateException("Unable to read Automation frame '" + outputVariable + "'.", e);
			}
			if (rows.size() != limit) {
				throw new IllegalStateException("Automation frame '" + outputVariable + "' changed while it was read.");
			}
			return rows;
		}

		String promptJson() {
			StringBuilder json = new StringBuilder("[");
			boolean first = true;
			for (long offset = 0; offset < rowCount();) {
				int pageSize = Math.toIntExact(Math.min(1000, rowCount() - offset));
				for (Object row : page(offset, pageSize)) {
					if (!first) {
						json.append(',');
					}
					json.append(AutomationRuntimeUtils.toRuntimeJson(row));
					first = false;
				}
				offset += pageSize;
			}
			return json.append(']').toString();
		}
	}
}
