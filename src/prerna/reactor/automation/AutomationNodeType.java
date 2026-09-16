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

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;

import prerna.engine.api.IEngine;

/**
 * Stable node identifiers and server-owned capabilities for Automation graphs.
 *
 * <p>
 * The {@link #getType()} value is the persisted wire contract. Enum names may be
 * refactored, but wire values must remain stable so saved Automation definitions
 * continue to load.
 */
public enum AutomationNodeType {

	TRIGGER_START(AutomationConstants.NODE_START, Category.TRIGGER, null, Permission.NONE, false, false),
	DATABASE_QUERY(AutomationConstants.NODE_DATABASE_QUERY, Category.DATABASE, IEngine.CATALOG_TYPE.DATABASE,
			Permission.VIEW, true, true),
	DATABASE_INSERT(AutomationConstants.NODE_DATABASE_INSERT, Category.DATABASE, IEngine.CATALOG_TYPE.DATABASE,
			Permission.EDIT, true, true),
	DATABASE_UPDATE(AutomationConstants.NODE_DATABASE_UPDATE, Category.DATABASE, IEngine.CATALOG_TYPE.DATABASE,
			Permission.EDIT, true, true),
	MODEL_CHAT(AutomationConstants.NODE_MODEL_CHAT, Category.MODEL, IEngine.CATALOG_TYPE.MODEL, Permission.VIEW,
			true, true),
	MODEL_EMBEDDINGS(AutomationConstants.NODE_MODEL_EMBEDDINGS, Category.MODEL, IEngine.CATALOG_TYPE.MODEL,
			Permission.VIEW, true, true),
	MODEL_VISION(AutomationConstants.NODE_MODEL_VISION, Category.MODEL, IEngine.CATALOG_TYPE.MODEL, Permission.VIEW,
			true, true),
	MODEL_NER(AutomationConstants.NODE_MODEL_NER, Category.MODEL, IEngine.CATALOG_TYPE.MODEL, Permission.VIEW,
			true, true),
	STORAGE_LIST(AutomationConstants.NODE_STORAGE_LIST, Category.STORAGE, IEngine.CATALOG_TYPE.STORAGE,
			Permission.VIEW, true, true),
	STORAGE_READ(AutomationConstants.NODE_STORAGE_READ, Category.STORAGE, IEngine.CATALOG_TYPE.STORAGE,
			Permission.EDIT, true, true),
	STORAGE_UPLOAD(AutomationConstants.NODE_STORAGE_UPLOAD, Category.STORAGE, IEngine.CATALOG_TYPE.STORAGE,
			Permission.EDIT, true, true),
	STORAGE_DOWNLOAD(AutomationConstants.NODE_STORAGE_DOWNLOAD, Category.STORAGE, IEngine.CATALOG_TYPE.STORAGE,
			Permission.VIEW, true, true),
	STORAGE_DELETE(AutomationConstants.NODE_STORAGE_DELETE, Category.STORAGE, IEngine.CATALOG_TYPE.STORAGE,
			Permission.EDIT, true, true),
	VECTOR_SEARCH(AutomationConstants.NODE_VECTOR_SEARCH, Category.VECTOR, IEngine.CATALOG_TYPE.VECTOR,
			Permission.VIEW, true, true),
	VECTOR_ADD(AutomationConstants.NODE_VECTOR_ADD, Category.VECTOR, IEngine.CATALOG_TYPE.VECTOR, Permission.EDIT,
			true, true),
	VECTOR_DELETE(AutomationConstants.NODE_VECTOR_DELETE, Category.VECTOR, IEngine.CATALOG_TYPE.VECTOR,
			Permission.EDIT, true, true),
	FUNCTION_EXECUTE(AutomationConstants.NODE_FUNCTION_EXECUTE, Category.FUNCTION, IEngine.CATALOG_TYPE.FUNCTION,
			Permission.VIEW, true, true),
	APP_PIXEL(AutomationConstants.NODE_APP_PIXEL, Category.APP, null, Permission.VIEW, true, true),
	AGENT_RUN(AutomationConstants.NODE_AGENT_RUN, Category.AGENT, IEngine.CATALOG_TYPE.MODEL, Permission.VIEW, true,
			true),
	CONTROL_WAIT(AutomationConstants.NODE_CONTROL_WAIT, Category.CONTROL, null, Permission.NONE, true, true),
	CONTROL_IF(AutomationConstants.NODE_CONTROL_IF, Category.CONTROL, null, Permission.NONE, false, false),
	DEVELOPER_PYTHON(AutomationConstants.NODE_DEVELOPER_PYTHON, Category.DEVELOPER, null, Permission.NONE, true,
			true);

	/** Logical grouping used by clients without parsing the persisted type string. */
	public enum Category {
		TRIGGER, DATABASE, MODEL, STORAGE, VECTOR, FUNCTION, APP, AGENT, CONTROL, DEVELOPER
	}

	/** Minimum resource permission required by the node when it references one. */
	public enum Permission {
		NONE, VIEW, EDIT
	}

	private static final Map<String, AutomationNodeType> TYPES_BY_VALUE;

	static {
		Map<String, AutomationNodeType> typesByValue = new LinkedHashMap<>();
		for (AutomationNodeType nodeType : values()) {
			AutomationNodeType previous = typesByValue.put(nodeType.type, nodeType);
			if (previous != null) {
				throw new IllegalStateException("Duplicate Automation node type: " + nodeType.type);
			}
		}
		TYPES_BY_VALUE = Collections.unmodifiableMap(typesByValue);
	}

	private final String type;
	private final Category category;
	private final IEngine.CATALOG_TYPE engineType;
	private final Permission permission;
	private final boolean supportsOutput;
	private final boolean supportsCustomCode;

	AutomationNodeType(String type, Category category, IEngine.CATALOG_TYPE engineType, Permission permission,
			boolean supportsOutput, boolean supportsCustomCode) {
		this.type = type;
		this.category = category;
		this.engineType = engineType;
		this.permission = permission;
		this.supportsOutput = supportsOutput;
		this.supportsCustomCode = supportsCustomCode;
	}

	/**
	 * @return stable value persisted in {@code automation-workflow.json}
	 */
	public String getType() {
		return this.type;
	}

	/**
	 * @return semantic category used by authoring clients
	 */
	public Category getCategory() {
		return this.category;
	}

	/**
	 * @return required engine catalog type, or {@code null} for non-engine nodes
	 */
	public IEngine.CATALOG_TYPE getEngineType() {
		return this.engineType;
	}

	/**
	 * @return minimum referenced-resource permission
	 */
	public Permission getPermission() {
		return this.permission;
	}

	/**
	 * @return {@code true} when this node may define an output variable
	 */
	public boolean supportsOutput() {
		return this.supportsOutput;
	}

	/**
	 * @return {@code true} when generated source may be replaced with custom source
	 */
	public boolean supportsCustomCode() {
		return this.supportsCustomCode;
	}

	/**
	 * Resolves a persisted type without relying on string prefixes.
	 *
	 * @param value persisted node type
	 * @return matching node type
	 * @throws IllegalArgumentException when the value is unsupported
	 */
	public static AutomationNodeType fromType(String value) {
		AutomationNodeType nodeType = TYPES_BY_VALUE.get(value);
		if (nodeType == null) {
			throw new IllegalArgumentException("Unsupported Automation node type: " + value);
		}
		return nodeType;
	}

	/**
	 * @param value persisted node type
	 * @return {@code true} when the value is a supported Automation node type
	 */
	public static boolean isSupported(String value) {
		return TYPES_BY_VALUE.containsKey(value);
	}
}
