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
package prerna.reactor.agent;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.GenRowStruct;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;

/**
 * Updates {@code .agents/AGENT_CONFIG.json} for a project. Each engine-type key
 * is optional — only the keys that are present are replaced; omitted types are
 * left untouched. Each value must be a list of maps shaped
 * {@code { "name": "...", "id": "..." }}.
 */
public class UpdateAppBuilderConfigReactor extends AbstractReactor {

    public UpdateAppBuilderConfigReactor() {
        this.keysToGet = new String[] {
                ReactorKeysEnum.PROJECT.getKey(),
                AppBuilderHarnessConfiguration.MODEL_ENGINES,
                AppBuilderHarnessConfiguration.VECTOR_ENGINES,
                AppBuilderHarnessConfiguration.STORAGE_ENGINES,
                AppBuilderHarnessConfiguration.DATABASE_ENGINES
        };
        this.keyRequired = new int[] { 1, 0, 0, 0, 0 };
    }

    @Override
    public NounMetadata execute() {
        organizeKeys();
        String projectId = this.keyValue.get(ReactorKeysEnum.PROJECT.getKey());

        Map<String, List<Map<String, String>>> updates = new LinkedHashMap<>();
        addIfPresent(updates, AppBuilderHarnessConfiguration.MODEL_ENGINES);
        addIfPresent(updates, AppBuilderHarnessConfiguration.VECTOR_ENGINES);
        addIfPresent(updates, AppBuilderHarnessConfiguration.STORAGE_ENGINES);
        addIfPresent(updates, AppBuilderHarnessConfiguration.DATABASE_ENGINES);

        String clientPath = AppBuildingHarness.resolveProjectClientPath(projectId);
        Boolean response = AppBuilderHarnessConfiguration.setSelectedEngines(clientPath, updates);
        return new NounMetadata(response, PixelDataType.BOOLEAN, PixelOperationType.OPERATION);
    }

    /**
     * Reads the engine list for {@code key} from the noun store. Adds it to
     * {@code updates} only when the key was actually supplied (so an absent key
     * leaves the existing value alone, while an explicit empty list clears it).
     */
    private void addIfPresent(Map<String, List<Map<String, String>>> updates, String key) {
        GenRowStruct grs = this.store.getNoun(key);
        if (grs == null || grs.isEmpty()) return;
        updates.put(key, extractEngineList(grs));
    }

    @SuppressWarnings("unchecked")
    private List<Map<String, String>> extractEngineList(GenRowStruct grs) {
        List<Map<String, String>> out = new ArrayList<>();
        List<NounMetadata> mapNouns = grs.getNounsOfType(PixelDataType.MAP);
        if (mapNouns != null) {
            for (NounMetadata n : mapNouns) {
                addEntry(out, (Map<String, Object>) n.getValue());
            }
        }
        List<NounMetadata> vecNouns = grs.getNounsOfType(PixelDataType.VECTOR);
        if (vecNouns != null) {
            for (NounMetadata n : vecNouns) {
                Object v = n.getValue();
                if (v instanceof List) {
                    for (Object item : (List<?>) v) {
                        if (item instanceof Map) {
                            addEntry(out, (Map<String, Object>) item);
                        }
                    }
                }
            }
        }
        return out;
    }

    private void addEntry(List<Map<String, String>> out, Map<String, Object> entry) {
        if (entry == null) return;
        Object id = entry.get(AppBuilderHarnessConfiguration.ENGINE_ID);
        if (id == null) return;
        Object name = entry.get(AppBuilderHarnessConfiguration.ENGINE_NAME);
        Map<String, String> m = new LinkedHashMap<>();
        m.put(AppBuilderHarnessConfiguration.ENGINE_NAME, name != null ? String.valueOf(name) : String.valueOf(id));
        m.put(AppBuilderHarnessConfiguration.ENGINE_ID,   String.valueOf(id));
        out.add(m);
    }
}
