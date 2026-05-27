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
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.attribute.BasicFileAttributes;
import java.text.SimpleDateFormat;
import java.util.Date;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.nounmeta.NounMetadata;

/**
 * Returns metadata for a file or directory inside the working directory.
 *
 * <p>Returns size in bytes, type (file/directory/symlink), permissions (readable/writable/
 * executable), created/modified timestamps, and (for directories) the count of immediate children.
 *
 * <p>This is the {@code stat} analogue — useful when an agent needs to make decisions based on
 * size, age, or existence without reading the file. Use {@link ReadFileReactor} for contents and
 * {@link ListDirectoryReactor} for directory listings.
 */
public class FileInfoReactor extends AbstractAgentToolReactor {

    public FileInfoReactor() {
        this.keysToGet   = new String[] { "path" };
        this.keyRequired = new int[]    { 1      };
    }

    @Override
    protected NounMetadata doExecute() throws Exception {
        String path = this.keyValue.get("path");
        if (path == null || path.trim().isEmpty()) {
            return new NounMetadata("Error: path is required", PixelDataType.CONST_STRING);
        }
        File f = resolveAndValidate(path);
        if (!f.exists()) {
            return new NounMetadata(
                    "path: " + toRelative(f.getAbsolutePath()) + "\nexists: false",
                    PixelDataType.CONST_STRING);
        }

        BasicFileAttributes attrs;
        try {
            attrs = Files.readAttributes(f.toPath(), BasicFileAttributes.class,
                    LinkOption.NOFOLLOW_LINKS);
        } catch (Exception e) {
            return new NounMetadata("Error: failed to stat: " + e.getMessage(),
                    PixelDataType.CONST_STRING);
        }

        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        String type;
        if (attrs.isSymbolicLink())      type = "symlink";
        else if (attrs.isDirectory())    type = "directory";
        else if (attrs.isRegularFile())  type = "file";
        else                             type = "other";

        StringBuilder sb = new StringBuilder();
        sb.append("path: ").append(toRelative(f.getAbsolutePath())).append('\n');
        sb.append("exists: true\n");
        sb.append("type: ").append(type).append('\n');
        sb.append("size_bytes: ").append(attrs.size()).append('\n');
        sb.append("created: ").append(sdf.format(new Date(attrs.creationTime().toMillis())))
          .append('\n');
        sb.append("modified: ").append(sdf.format(new Date(attrs.lastModifiedTime().toMillis())))
          .append('\n');
        sb.append("readable: ").append(f.canRead()).append('\n');
        sb.append("writable: ").append(f.canWrite()).append('\n');
        sb.append("executable: ").append(f.canExecute()).append('\n');

        if (attrs.isDirectory()) {
            File[] children = f.listFiles();
            sb.append("child_count: ").append(children == null ? 0 : children.length).append('\n');
        }
        return new NounMetadata(sb.toString().trim(), PixelDataType.CONST_STRING);
    }

    @Override
    protected String getDescriptionForKey(String key) {
        if ("path".equals(key)) {
            return "Relative path of the file or directory to inspect (within the working directory).";
        }
        return super.getDescriptionForKey(key);
    }

    @Override
    public String getReactorDescription() {
        return "Returns metadata for a path: type (file|directory|symlink|other), size, created/"
             + "modified timestamps, permission flags, and (for directories) immediate child count. "
             + "Reports exists=false rather than erroring on missing paths.";
    }
}
