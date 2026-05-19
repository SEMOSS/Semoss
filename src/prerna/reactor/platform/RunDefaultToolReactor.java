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
package prerna.reactor.platform;

import java.util.List;
import java.util.Map;

import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.GenRowStruct;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;

/**
 * Pixel entrypoint used by the frontend to invoke a {@code platform__*} tool
 * after the LLM returns a tool call. Delegates to
 * {@link PlatformDefaultTools#execute}.
 *
 * <pre>RunDefaultTool(function=["platform__Command"], paramValues=[{"command": "ls -la"}]);</pre>
 */
public class RunDefaultToolReactor extends AbstractReactor {

    public RunDefaultToolReactor() {
        this.keysToGet = new String[] {
                ReactorKeysEnum.FUNCTION.getKey(),
                ReactorKeysEnum.PARAM_VALUES_MAP.getKey()
        };
        this.keyRequired = new int[] { 1, 0 };
    }

    @Override
    public NounMetadata execute() {
        organizeKeys();

        String toolName = this.keyValue.get(ReactorKeysEnum.FUNCTION.getKey());
        if (toolName == null || (toolName = toolName.trim()).isEmpty()) {
            throw new IllegalArgumentException("Tool function name must be provided");
        }

        Map<String, Object> paramMap = getParamMap();
        return PlatformDefaultTools.execute(toolName, paramMap, this.insight);
    }

    private Map<String, Object> getParamMap() {
        GenRowStruct mapGrs = this.store.getGenRowStruct(ReactorKeysEnum.PARAM_VALUES_MAP.getKey());
        if (mapGrs != null && !mapGrs.isEmpty()) {
            List<NounMetadata> mapInputs = mapGrs.getNounsOfType(PixelDataType.MAP);
            if (mapInputs != null && !mapInputs.isEmpty()) {
                @SuppressWarnings("unchecked")
                Map<String, Object> m = (Map<String, Object>) mapInputs.get(0).getValue();
                return m;
            }
        }
        List<NounMetadata> mapInputs = this.curRow.getNounsOfType(PixelDataType.MAP);
        if (mapInputs != null && !mapInputs.isEmpty()) {
            @SuppressWarnings("unchecked")
            Map<String, Object> m = (Map<String, Object>) mapInputs.get(0).getValue();
            return m;
        }
        return null;
    }

    @Override
    public String getReactorDescription() {
        return "Execute a platform default tool by its prefixed name (e.g. platform__Command)";
    }

    @Override
    protected String getDescriptionForKey(String key) {
        if (key.equals(ReactorKeysEnum.FUNCTION.getKey())) {
            return "The platform tool name, including the platform__ prefix (e.g. platform__Command)";
        } else if (key.equals(ReactorKeysEnum.PARAM_VALUES_MAP.getKey())) {
            return "A key-value map of parameter inputs for the tool";
        }
        return super.getDescriptionForKey(key);
    }
}
