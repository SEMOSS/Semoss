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

import java.util.List;
import java.util.Set;

import prerna.om.Insight;
import prerna.sablecc2.PixelRunner;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.nounmeta.NounMetadata;

/**
 * Runs a validated Pixel expression in the run Insight with active cancellation.
 */
final class AutomationPixelExecutor {

	private static final Set<PixelOperationType> ERROR_OPERATIONS = Set.of(
			PixelOperationType.ERROR,
			PixelOperationType.UNEXECUTED_PIXELS,
			PixelOperationType.FRAME_SIZE_LIMIT_EXCEEDED,
			PixelOperationType.USER_INPUT_REQUIRED,
			PixelOperationType.LOGGIN_REQUIRED_ERROR,
			PixelOperationType.ANONYMOUS_USER_ERROR,
			PixelOperationType.INVALID_SYNTAX);

	private final Insight insight;
	private final String runId;
	private final String registryKey;

	AutomationPixelExecutor(Insight insight, String runId) {
		this(insight, runId, runId);
	}

	AutomationPixelExecutor(Insight insight, String runId, String registryKey) {
		this.insight = insight;
		this.runId = runId;
		this.registryKey = registryKey;
	}

	Object execute(String expression) {
		if (expression == null || expression.isBlank()) {
			throw new IllegalArgumentException("Automation Pixel expression must not be blank.");
		}
		if (!expression.trim().endsWith(";")) {
			throw new IllegalArgumentException("Automation Pixel expression must end with ';'.");
		}
		PixelRunner runner = insight.getPixelRunner();
		Runnable cancellationSignal = runner::cancelRequest;
		AutomationRunRegistry.attachJavaCancellation(registryKey, cancellationSignal);
		try {
			insight.runPixel(runner, expression);
		} finally {
			AutomationRunRegistry.detachJavaCancellation(registryKey, cancellationSignal);
		}
		if (runner.isCancelRequested() || AutomationRunRegistry.isCancellationRequested(runId)) {
			throw new IllegalStateException("Run cancelled by user");
		}
		List<NounMetadata> results = runner.getResults();
		if (results == null || results.isEmpty()) {
			throw new IllegalStateException("Automation Pixel expression returned no result.");
		}
		NounMetadata result = results.get(results.size() - 1);
		if (hasErrorOperation(result)) {
			throw new IllegalStateException(String.valueOf(result.getValue()));
		}
		PixelDataType nounType = result.getNounType();
		if (nounType == PixelDataType.PIXEL_RUNNER || nounType == PixelDataType.FRAME
				|| nounType == PixelDataType.FRAME_MAP || nounType == PixelDataType.TASK
				|| nounType == PixelDataType.TASK_LIST || nounType == PixelDataType.FORMATTED_DATA_SET) {
			throw new IllegalStateException(
					"Automation Pixel expression returned unsupported result type " + nounType + ".");
		}
		return result.getValue();
	}

	private static boolean hasErrorOperation(NounMetadata result) {
		if (result.getOpType().stream().anyMatch(ERROR_OPERATIONS::contains)) {
			return true;
		}
		return result.getAdditionalReturn() != null && result.getAdditionalReturn().stream()
				.anyMatch(AutomationPixelExecutor::hasErrorOperation);
	}
}
