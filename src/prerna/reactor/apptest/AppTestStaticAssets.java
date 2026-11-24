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

import java.net.URI;
import java.util.List;
import java.util.Locale;
import java.util.Set;

import prerna.project.impl.ProjectPortalsHelper;

/** URL-only policy for public frontend assets. Never permits an ordinary backend endpoint. */
final class AppTestStaticAssets {
    static final Set<String> EXTENSIONS = Set.of("js", "mjs", "css", "json", "svg", "png", "jpg", "jpeg", "gif", "webp", "ico", "woff", "woff2", "ttf", "otf");
    static final int MAX_BYTES = 16 * 1024 * 1024;

    private AppTestStaticAssets() { }

    static String sdkEntry() { return ProjectPortalsHelper.resolveSdkEntryUrl(); }

    static List<String> roots() {
        String entry = sdkEntry();
        String suffix = "libs/sdk/dist/index.mjs";
        if (!entry.endsWith(suffix)) throw new IllegalArgumentException("Invalid platform SDK URL");
        String frontend = entry.substring(0, entry.length() - suffix.length());
        if (!canonical(frontend)) throw new IllegalArgumentException("Invalid frontend asset URL");
        return List.of(frontend + "libs/", frontend + "packages/", frontend + "assets/");
    }

    static boolean allows(URI origin, URI url, String method) {
        String path = url.getRawPath();
        if (!"GET".equals(method) || !("http".equals(url.getScheme()) || "https".equals(url.getScheme()))
                || url.getUserInfo() != null || !AppTestRunner.sameOrigin(origin, url) || !canonical(path)) return false;
        String extension = path.substring(path.lastIndexOf('.') + 1).toLowerCase(Locale.ROOT);
        return EXTENSIONS.contains(extension) && roots().stream().anyMatch(path::startsWith);
    }

    private static boolean canonical(String path) {
        if (path == null || !path.startsWith("/") || path.contains("//") || !path.matches("[A-Za-z0-9_./@~-]+")) return false;
        for (String part : path.split("/"))
            if (part.equals(".") || part.equals("..") || part.equalsIgnoreCase("api")) return false;
        return true;
    }

    static String sources(String moduleUrl) {
        URI origin = URI.create(moduleUrl);
        return String.join(" ", roots().stream().map(path -> origin.resolve(path).toString()).toList());
    }

    static boolean validContentType(String url, String contentType) {
        String type = contentType == null ? "" : contentType.toLowerCase(Locale.ROOT).split(";", 2)[0].trim();
        String path = URI.create(url).getPath().toLowerCase(Locale.ROOT);
        if (path.endsWith(".js") || path.endsWith(".mjs")) return type.contains("javascript") || type.contains("ecmascript");
        if (path.endsWith(".css")) return type.equals("text/css");
        if (path.endsWith(".json")) return type.equals("application/json") || type.endsWith("+json");
        return type.startsWith("image/") || type.startsWith("font/") || type.equals("application/octet-stream")
                || type.startsWith("application/font") || type.equals("application/vnd.ms-fontobject");
    }
}
