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
package prerna.reactor.interceptor;

/**
 * A utility class to hold the constant keys for the keyValue map passed to
 * pipeline reactors.
 */
public final class PipelineReactorUtils {

	public static final String INPUT_REACTOR_NAME = "inputReactorName";
	public static final String OUTPUT_REACTOR_NAME = "outputReactorName";
	public static final String IS_SUCCESS = "isSuccess";

	// public static final String REACTOR_SPAN_ID = "reactorSpanId";
	public static final String METHOD_NAME = "methodName";
	public static final String ARGUMENTS = "arguments";
	public static final String RESULT = "result";
	public static final String INTERIM_RESULT = "interim_result";
	public static final String CONFIG = "config";
	public static final String TARGET_PARAM = "target_param";
	public static final String INTERCEPTOR = "interceptor";
	public static final String PASS = "pass";
	public static final String PASS_DETAILS = "passDetails";
	public static final String MASKED = "masked";
	public static final String SHORT_CIRCUIT_RESPONSE = "shortCircuitResponse";
	public static final String CLOSE_ROOM = "closeRoom";
	public static final String BLOCK_ERROR_MESSAGE = "blockErrorMessage";

	private PipelineReactorUtils() {
		// private constructor to prevent instantiation
	}
}
