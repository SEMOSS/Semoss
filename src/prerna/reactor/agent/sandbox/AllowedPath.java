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
package prerna.reactor.agent.sandbox;

import java.nio.file.Path;
import java.util.Objects;

/**
 * Immutable (absolute path, access mode) pair in a {@link SandboxPolicy}.
 *
 * <p>Produced only by {@link SandboxPolicyBuilder#build()}; paths are
 * normalized and verified absolute at construction time.
 */
public final class AllowedPath {

    private final Path path;
    private final AccessMode mode;

    AllowedPath(Path path, AccessMode mode) {
        if (path == null || !path.isAbsolute()) {
            throw new IllegalArgumentException("AllowedPath requires an absolute path: " + path);
        }
        this.path = path.normalize();
        this.mode = Objects.requireNonNull(mode, "mode");
    }

    public Path getPath() {
        return path;
    }

    public AccessMode getMode() {
        return mode;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof AllowedPath)) return false;
        AllowedPath other = (AllowedPath) o;
        return path.equals(other.path) && mode == other.mode;
    }

    @Override
    public int hashCode() {
        return Objects.hash(path, mode);
    }

    @Override
    public String toString() {
        return mode + ":" + path;
    }
}
