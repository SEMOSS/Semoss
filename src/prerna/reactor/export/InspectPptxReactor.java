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
package prerna.reactor.export;

import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;

import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.GenRowStruct;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.AssetUtility;
import prerna.util.pptx.SemossPptxInspector;

/** Pixel entry point for the same inspection service exposed by the agent's InspectPptx tool. */
public class InspectPptxReactor extends AbstractReactor {
    public InspectPptxReactor() {
        this.keysToGet = new String[] { ReactorKeysEnum.FILE_PATH.getKey(), "instructions", "slides", "context",
                ReactorKeysEnum.ENGINE.getKey(), "checkConsistency", ReactorKeysEnum.SPACE.getKey() };
        this.keyRequired = new int[] { 1, 1, 0, 0, 0, 0, 0 };
    }

    @Override public NounMetadata execute() {
        organizeKeys();
        Map<String, Object> params = new HashMap<>();
        for (String key : keysToGet) {
            if (key.equals("space") || key.equals("slides")) continue;
            String value = keyValue.get(key);
            if (value != null) {
                if (key.equals("checkConsistency")) {
                    if (!value.equalsIgnoreCase("true") && !value.equalsIgnoreCase("false"))
                        throw new IllegalArgumentException("checkConsistency must be a boolean");
                    params.put(key, Boolean.parseBoolean(value));
                } else params.put(key, value);
            }
        }
        GenRowStruct slideValues = store.getNoun("slides");
        if (slideValues != null) {
            java.util.List<Object> slides = new java.util.ArrayList<>();
            for (int i = 0; i < slideValues.size(); i++) slides.add(slideValues.get(i));
            params.put("slides", slides);
        }
        // Inspection writes its PDF/images/report into this space, so require edit access.
        String root = AssetUtility.getRootFolderPath(insight, keyValue.get(ReactorKeysEnum.SPACE.getKey()), true);
        try {
            return new NounMetadata(SemossPptxInspector.inspect(Path.of(root), params, insight, insight.getRoomId(), null).toMap(), PixelDataType.MAP);
        } catch (RuntimeException e) { throw e;
        } catch (Exception e) { throw new IllegalArgumentException("PPTX inspection failed: " + e.getMessage(), e); }
    }

    @Override public String getReactorDescription() {
        return "Render all or selected PowerPoint slides with UnoServer and inspect their images with a vision model against instructions.";
    }
}
