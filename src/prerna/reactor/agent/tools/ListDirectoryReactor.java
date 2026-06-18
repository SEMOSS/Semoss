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
package prerna.reactor.agent.tools;

import java.io.File;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Date;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.nounmeta.NounMetadata;

/**
 * Lists the contents of a directory.
 *
 * <p>Directories are listed before files; both groups are sorted alphabetically. Each entry shows
 * type (DIR/FILE), last-modified timestamp, size in bytes (files only), and name.
 */
public class ListDirectoryReactor extends AbstractAgentToolReactor {

    public ListDirectoryReactor() {
        this.keysToGet = new String[]{"path"};
        this.keyRequired = new int[]{0};
    }

    @Override
    protected NounMetadata doExecute() throws Exception {
        String path = this.keyValue.get("path");
        File dir = resolveAndValidate(path);

        if (!dir.exists()) {
            return new NounMetadata(
                    "Error: directory not found: " + (path != null ? path : "."),
                    PixelDataType.CONST_STRING);
        }
        if (!dir.isDirectory()) {
            return new NounMetadata("Error: not a directory: " + path, PixelDataType.CONST_STRING);
        }

        File[] files = dir.listFiles();
        if (files == null || files.length == 0) {
            return new NounMetadata("(empty directory)", PixelDataType.CONST_STRING);
        }

        // Directories first, then files; each group sorted alphabetically
        Arrays.sort(files, (a, b) -> {
            if (a.isDirectory() != b.isDirectory()) return a.isDirectory() ? -1 : 1;
            return a.getName().compareToIgnoreCase(b.getName());
        });

        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm");
        StringBuilder sb = new StringBuilder();
        for (File f : files) {
            String type = f.isDirectory() ? "DIR " : "FILE";
            String size = f.isDirectory() ? "         " : String.format("%9d", f.length());
            sb.append(String.format("%s  %s  %s  %s%n",
                    type, sdf.format(new Date(f.lastModified())), size, f.getName()));
        }
        return new NounMetadata(sb.toString().trim(), PixelDataType.CONST_STRING);
    }

    @Override
    public String getReactorDescription() {
        return "Lists directory contents showing type, date, size, and name. "
             + "Directories listed first. Defaults to the working directory.";
    }
}
