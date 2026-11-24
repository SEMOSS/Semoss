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
package prerna.reactor.apptest;

import static org.junit.jupiter.api.Assertions.*;
import java.net.URI;
import org.junit.jupiter.api.Test;

class AppTestStaticAssetsUnitTests {
    private final URI origin = URI.create("https://platform.test/Monolith/");

    @Test void allowsOnlyFirstPartyStaticGetRequestsInFrontendAssetDirectories() {
        String root = AppTestStaticAssets.roots().getFirst();
        assertTrue(AppTestStaticAssets.allows(origin, origin.resolve(root + "sdk/dist/index.mjs?v=1"), "GET"));
        assertTrue(AppTestStaticAssets.allows(origin, origin.resolve(root + "ui/dist/style.css"), "GET"));
        assertFalse(AppTestStaticAssets.allows(origin, origin.resolve(root + "sdk/dist/index.mjs"), "POST"));
        for (String path : new String[]{"/Monolith/api/engine/runPixel", "/Monolith/api/delete.js", root + "api/write.js",
                root + "sdk/../api/write.js", root + "sdk/%2e%2e/api/write.js", root + "sdk//dist/index.mjs",
                root + "sdk/dist/index.mjs;action=write", root + "sdk/dist/index.html", "/unrelated/main.js"})
            assertFalse(AppTestStaticAssets.allows(origin, origin.resolve(path), "GET"), path);
        assertFalse(AppTestStaticAssets.allows(origin, URI.create("https://other.test"+root+"sdk/dist/index.mjs"), "GET"));
        assertFalse(AppTestStaticAssets.allows(origin, URI.create("https://user:password@platform.test"+root+"sdk/dist/index.mjs"), "GET"));
        assertFalse(AppTestStaticAssets.allows(origin, URI.create("https://platform.test:8443"+root+"sdk/dist/index.mjs"), "GET"));
    }

    @Test void doesNotTreatHtmlLoginOrErrorResponsesAsJavascript() {
        assertTrue(AppTestStaticAssets.validContentType("https://platform.test/index.mjs", "text/javascript;charset=utf-8"));
        assertFalse(AppTestStaticAssets.validContentType("https://platform.test/index.mjs", "text/html"));
        assertFalse(AppTestStaticAssets.validContentType("https://platform.test/index.mjs", "application/json"));
    }
}
