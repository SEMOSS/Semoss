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
package prerna.engine.impl.pipeline;

import java.util.LinkedHashMap;
import java.util.Map;

import prerna.sablecc2.om.execptions.SemossPixelException;

/**
 * A call to an engine or a project the access check refused, with what the
 * Activity Log and the audit trail record about it: the level the call needed
 * and who made it.
 */
public class EngineAccessRefusedException extends SemossPixelException {

	private static final long serialVersionUID = 1L;

	private final transient Map<String, Object> details;

	/**
	 * @param message the reason given to the caller
	 * @param details the level the call needed and who made it, such as
	 *                {@code levelNeeded}, {@code via}, {@code appId},
	 *                {@code agentId} and {@code callingEngineId}
	 */
	EngineAccessRefusedException(String message, Map<String, Object> details) {
		super(message);
		this.details = new LinkedHashMap<>(details);
	}

	/**
	 * What the Activity Log records about the refusal.
	 *
	 * @param attempt how many times the person was refused on the engine or project
	 *                in this session, this one included
	 * @return the decision, with reason {@link EngineAccessReason#REFUSED}
	 */
	public EngineAccessDecision decision(int attempt) {
		Map<String, Object> withAttempt = new LinkedHashMap<>(this.details);
		withAttempt.put("attempt", attempt);
		return new EngineAccessDecision(EngineAccessReason.REFUSED, null, withAttempt);
	}

	/**
	 * The level the call needed and who made it.
	 *
	 * @return the details, which leave out what was sent
	 */
	public Map<String, Object> getDetails() {
		return new LinkedHashMap<>(this.details);
	}

}
