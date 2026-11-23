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
package prerna.auth.utils;

import java.util.Locale;

/**
 * Why a catalog item depends on another, kept in CATALOGDEPENDENCIES.ROLE. Every
 * role but {@link #MANUAL} is written by a save reactor from the item's own
 * configuration, and only that reactor changes it; {@link #MANUAL} is a
 * dependency a user picked.
 */
public enum CatalogDependencyRole {

	/** Picked by a user with SetCatalogDependencies */
	MANUAL,

	/** The model an agent runs on */
	AGENT_MODEL,
	/** An agent another agent can delegate to */
	SUBAGENT,
	/** A skill attached to an agent */
	SKILL,
	/** An engine or MCP project an agent can call as a tool */
	TOOLBOX,
	/** A vector database an agent searches */
	KNOWLEDGE,

	/** A guardrail that checks calls to an engine */
	GUARDRAIL,
	/** The model a guardrail asks to evaluate the text */
	JUDGE_MODEL,
	/** The vector database an On Topic guardrail compares the text with */
	TOPIC_VECTOR,
	/** The model a vector database embeds with */
	EMBEDDER,
	/** The storage engine a function reads its bucket from */
	FUNCTION_STORAGE,
	/** The model a vector database generates keywords with */
	KEYWORD_ENGINE,
	/** The function a vector database processes its documents with */
	DOCUMENT_PROCESSOR,
	/** The vector database a proxy vector database sends its calls to */
	PROXY_TARGET,
	/** The model a function describes images with */
	IMAGE_MODEL,

	/** A model a router sends a route to */
	ROUTE_MODEL,
	/** The model a router classifies requests with */
	ROUTER_CLASSIFIER,
	/** The model a router embeds requests with */
	ROUTER_EMBEDDINGS,
	/** A model a router falls back to */
	ROUTER_FALLBACK,

	/** An engine, app or agent a node of an automation uses */
	AUTOMATION_NODE,
	/** An engine a block of a blocks app uses */
	BLOCK_ENGINE,
	/** An engine id found in an app's files */
	CODE_REFERENCE;

	/** Whether a save reactor writes this role, rather than a user */
	public boolean isAutomatic() {
		return this != MANUAL;
	}

	/**
	 * The role of a stored value
	 *
	 * @param value the stored ROLE, which may be null for a row from before roles
	 * @return the role, or {@link #MANUAL} when there is none or it is not known
	 */
	public static CatalogDependencyRole fromStored(String value) {
		if (value == null || value.isBlank()) {
			return MANUAL;
		}
		try {
			return valueOf(value.trim().toUpperCase(Locale.ROOT));
		} catch (IllegalArgumentException e) {
			return MANUAL;
		}
	}
}
