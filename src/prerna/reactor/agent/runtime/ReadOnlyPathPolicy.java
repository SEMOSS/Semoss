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
package prerna.reactor.agent.runtime;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Set;

/** Mutation guard for configured packaged resources, including ancestor moves and symlink aliases. */
final class ReadOnlyPathPolicy {
    private ReadOnlyPathPolicy() {}

    static void requireWritable(Path root, Path target, Set<String> readOnlyPaths) {
        if (readOnlyPaths == null || readOnlyPaths.isEmpty()) return;
        try {
            Path resolvedTarget = canonical(target);
            for (String relative : readOnlyPaths) {
                Path protectedPath = canonical(root.resolve(relative));
                if (resolvedTarget.startsWith(protectedPath) || protectedPath.startsWith(resolvedTarget)) {
                    throw new IllegalArgumentException("Packaged resource is read-only: " + relative
                            + ". Edit your deck-specific generator or use the component API; do not patch the helper library.");
                }
            }
        } catch (IOException e) {
            throw new IllegalArgumentException("Unable to verify protected file path", e);
        }
    }

    private static Path canonical(Path path) throws IOException {
        path = path.toAbsolutePath().normalize();
        if (Files.exists(path)) return path.toRealPath();
        Path parent = path.getParent();
        return parent == null ? path : canonical(parent).resolve(path.getFileName());
    }
}
