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
package prerna.usertracking;

import prerna.engine.api.IEngine;

/**
 * What an audit event was done to, stored in
 * {@code USER_AUDIT_EVENTS.TARGET_TYPE}.
 */
public enum AuditTargetType {

	/** A user account */
	USER,
	/** A team */
	GROUP,
	/** A project, which the UI calls an app */
	PROJECT,
	/** An insight in a project */
	INSIGHT,
	/** A workspace */
	WORKSPACE,
	/** An engine whose kind is not known */
	ENGINE,
	/** A database engine */
	DATABASE,
	/** A model engine */
	MODEL,
	/** A vector database engine */
	VECTOR,
	/** A storage engine */
	STORAGE,
	/** A function engine */
	FUNCTION,
	/** A guardrail engine */
	GUARDRAIL,
	/** A virtual environment engine */
	VENV,
	/** An automation */
	AUTOMATION,
	/** A skill */
	SKILL,
	/** A scheduled job */
	JOB,
	/** An API key */
	API_KEY,
	/** The whole platform, such as the General Platform Limit */
	PLATFORM,
	/** The platform's configuration */
	PLATFORM_CONFIG,
	/** A save of permission batches */
	PERMISSION_BATCH,
	/** The audit trail itself */
	AUDIT;

	/**
	 * Whether the target is an engine of any kind, as the ENGINE security tables
	 * hold them.
	 *
	 * @return true for an engine whose kind is known or not
	 */
	public boolean isEngine() {
		return switch (this) {
		case ENGINE, DATABASE, MODEL, VECTOR, STORAGE, FUNCTION, GUARDRAIL, VENV -> true;
		default -> false;
		};
	}

	/**
	 * Reads a target type stored by its name.
	 *
	 * @param name the stored name, or null
	 * @return the target type with the name, or null when no type has it
	 */
	public static AuditTargetType find(String name) {
		for (AuditTargetType type : values()) {
			if (type.name().equals(name)) {
				return type;
			}
		}
		return null;
	}

	/**
	 * The target type of an engine, by its catalog type.
	 *
	 * @param type the engine's catalog type, or null when it is not known
	 * @return the matching target type, or {@link #ENGINE} when the type is not
	 *         known
	 */
	public static AuditTargetType ofEngine(IEngine.CATALOG_TYPE type) {
		if (type == null) {
			return ENGINE;
		}
		return switch (type) {
		case DATABASE -> DATABASE;
		case STORAGE -> STORAGE;
		case MODEL -> MODEL;
		case VECTOR -> VECTOR;
		case FUNCTION -> FUNCTION;
		case GUARDRAIL -> GUARDRAIL;
		case VENV -> VENV;
		case PROJECT -> PROJECT;
		};
	}
}
