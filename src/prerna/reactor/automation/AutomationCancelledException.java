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
package prerna.reactor.automation;

/**
 * Thrown mid-node when a cancellation request is detected during a blocking operation.
 *
 * <p>Using a distinct unchecked exception type (rather than a flag return value or a checked
 * exception) lets nodes that loop internally - such as {@code WaitNodeExecutor} sleeping in
 * chunks - abort cleanly without threading a cancellation result through every call frame. The
 * caller ({@code TriggerAutomationReactor.executeSingleNode}) catches this type specifically and
 * records the run as {@link AutomationConstants#STATUS_CANCELLED} instead of
 * {@link AutomationConstants#STATUS_FAILED}, so the end-user sees the correct terminal state.
 */
public class AutomationCancelledException extends RuntimeException {

	private static final long serialVersionUID = 1L;

	public AutomationCancelledException(String message) {
		super(message);
	}
}
