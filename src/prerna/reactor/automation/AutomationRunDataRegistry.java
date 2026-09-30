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
 * -----------------------------------------------------------------------------
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

import java.util.List;
import java.util.Map;

import prerna.om.Insight;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.VarStore;
import prerna.sablecc2.om.nounmeta.NounMetadata;

/**
 * Run-Insight registry for opaque Automation data references.
 *
 * <p>
 * The registry is deliberately held in the execution {@link Insight}'s
 * {@link VarStore}, alongside the other Insight-owned runtime values. It records
 * only ownership and the private backing location; node scope continues to hold
 * the provider-neutral {@link AutomationDataReference}. This keeps the user
 * contract stable while allowing different node implementations to retain data
 * as a task, Python value, frame, or another future backing.
 */
final class AutomationRunDataRegistry {

	private static final String VARIABLE_PREFIX = "__automation_data__";

	/** Private backing kinds. These values never cross the node or client contract. */
	enum Backing {
		RUN_MEMORY, TASK
	}

	/** One Insight-owned routing entry for an opaque reference. */
	record Entry(AutomationDataOwner owner, AutomationValueType valueType, Backing backing, String resourceId) {
		Entry {
			if (owner == null || valueType == null || valueType == AutomationValueType.UNKNOWN || backing == null
					|| resourceId == null || resourceId.isBlank()) {
				throw new IllegalArgumentException("Automation run data registry entry is incomplete.");
			}
		}
	}

	private AutomationRunDataRegistry() {
	}

	/** Registers a provider-owned value in its execution Insight. */
	static void register(Insight insight, AutomationDataReference reference, AutomationDataOwner owner, Backing backing,
			String resourceId) {
		validateInsight(insight, owner);
		Entry requested = new Entry(owner, reference.valueType(), backing, resourceId);
		VarStore store = insight.getVarStore();
		synchronized (store) {
			NounMetadata current = store.get(variable(reference.referenceId()));
			if (current != null) {
				Entry existing = entry(current);
				if (!existing.equals(requested)) {
					throw new IllegalStateException("Automation data reference is already registered to another value.");
				}
				return;
			}
			store.put(variable(reference.referenceId()),
					new NounMetadata(requested, PixelDataType.CUSTOM_DATA_STRUCTURE));
		}
	}

	/**
	 * Registers references produced by the run-local Python provider. Existing
	 * provider registrations win, which preserves task-backed query references
	 * that pass through the same node result boundary.
	 */
	static void registerRunMemoryReferences(Insight insight, Object value, AutomationDataOwner owner) {
		AutomationDataReference reference = AutomationDataReference.fromValue(value);
		if (reference != null) {
			registerRunMemoryIfAbsent(insight, reference, owner);
			return;
		}
		if (value instanceof Map<?, ?> map) {
			for (Object item : map.values()) {
				registerRunMemoryReferences(insight, item, owner);
			}
		} else if (value instanceof List<?> list) {
			for (Object item : list) {
				registerRunMemoryReferences(insight, item, owner);
			}
		}
	}

	/** Resolves and authorizes one private backing entry. */
	static Entry require(Insight insight, AutomationDataReference reference, AutomationDataOwner owner) {
		validateInsight(insight, owner);
		NounMetadata noun = insight.getVarStore().get(variable(reference.referenceId()));
		if (noun == null) {
			throw new IllegalStateException("Automation data is no longer available in this run workspace.");
		}
		Entry registered = entry(noun);
		if (!registered.owner().equals(owner) || registered.valueType() != reference.valueType()) {
			throw new SecurityException("Automation data reference does not belong to this execution owner.");
		}
		return registered;
	}

	private static void registerRunMemoryIfAbsent(Insight insight, AutomationDataReference reference,
			AutomationDataOwner owner) {
		validateInsight(insight, owner);
		VarStore store = insight.getVarStore();
		synchronized (store) {
			NounMetadata current = store.get(variable(reference.referenceId()));
			if (current != null) {
				Entry existing = entry(current);
				if (!existing.owner().equals(owner) || existing.valueType() != reference.valueType()) {
					throw new SecurityException("Automation data reference does not belong to this execution owner.");
				}
				return;
			}
			Entry entry = new Entry(owner, reference.valueType(), Backing.RUN_MEMORY, reference.referenceId());
			store.put(variable(reference.referenceId()), new NounMetadata(entry, PixelDataType.CUSTOM_DATA_STRUCTURE));
		}
	}

	private static Entry entry(NounMetadata noun) {
		if (noun.getNounType() != PixelDataType.CUSTOM_DATA_STRUCTURE || !(noun.getValue() instanceof Entry entry)) {
			throw new IllegalStateException("Automation run data registry contains an invalid entry.");
		}
		return entry;
	}

	private static String variable(String referenceId) {
		return VARIABLE_PREFIX + referenceId;
	}

	private static void validateInsight(Insight insight, AutomationDataOwner owner) {
		if (insight == null || owner == null
				|| !AutomationRunExecutionService.isExecutionInsight(insight, owner.runId())
				|| !owner.projectId().equals(insight.getProjectId())) {
			throw new IllegalArgumentException("Automation data is available only inside its owning execution Insight.");
		}
	}
}
