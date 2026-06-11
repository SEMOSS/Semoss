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

import java.nio.file.InvalidPathException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

/**
 * Fluent builder for {@link SandboxPolicy}.
 *
 * <p>Each path is normalized via {@link Path#toAbsolutePath()} then
 * {@link Path#normalize()} (which collapses {@code .} and {@code ..} segments)
 * and deduplicated by the resulting absolute form.  If the same directory is
 * added as both {@link AccessMode#RO} and {@link AccessMode#RW}, RW wins.
 */
public final class SandboxPolicyBuilder {

    // insertion-order map keeps output stable for tests / debugging
    private final Map<Path, AccessMode> paths        = new LinkedHashMap<>();
    private final Set<Path>             blockedPaths = new LinkedHashSet<>();
    private Path             tmpDir;
    private boolean          loopbackNetwork = true;
    private EnforcementMode  enforcement     = EnforcementMode.ENFORCE;

    SandboxPolicyBuilder() {}

    public SandboxPolicyBuilder withRead(Path path) {
        addPath(path, AccessMode.RO);
        return this;
    }

    public SandboxPolicyBuilder withRead(String path) {
        return withRead(toPath(path));
    }

    public SandboxPolicyBuilder withReadWrite(Path path) {
        addPath(path, AccessMode.RW);
        return this;
    }

    public SandboxPolicyBuilder withReadWrite(String path) {
        return withReadWrite(toPath(path));
    }

    public SandboxPolicyBuilder withTmpDir(Path tmpDir) {
        this.tmpDir = tmpDir != null ? tmpDir.toAbsolutePath().normalize() : null;
        return this;
    }

    public SandboxPolicyBuilder withBlock(Path path) {
        if (path == null) {
            throw new IllegalArgumentException("blocked path must not be null");
        }
        blockedPaths.add(path.toAbsolutePath().normalize());
        return this;
    }

    public SandboxPolicyBuilder withBlock(String path) {
        return withBlock(toPath(path));
    }

    public SandboxPolicyBuilder withLoopbackNetwork(boolean allow) {
        this.loopbackNetwork = allow;
        return this;
    }

    public SandboxPolicyBuilder withEnforcement(EnforcementMode mode) {
        this.enforcement = mode != null ? mode : EnforcementMode.ENFORCE;
        return this;
    }

    public SandboxPolicy build() {
        List<AllowedPath> list = new ArrayList<>(paths.size());
        for (Map.Entry<Path, AccessMode> e : paths.entrySet()) {
            list.add(new AllowedPath(e.getKey(), e.getValue()));
        }
        return new SandboxPolicy(list, new ArrayList<>(blockedPaths),
                Optional.ofNullable(tmpDir), loopbackNetwork, enforcement);
    }

    // helpers

    private void addPath(Path path, AccessMode mode) {
        if (path == null) {
            throw new IllegalArgumentException("sandbox path must not be null");
        }
        Path abs = path.toAbsolutePath().normalize();
        // RW wins over RO - a later RO request must not downgrade an existing RW.
        if (paths.get(abs) == AccessMode.RW) {
            return;
        }
        paths.put(abs, mode);
    }

    private static Path toPath(String path) {
        if (path == null || path.trim().isEmpty()) {
            throw new IllegalArgumentException("sandbox path must not be empty");
        }
        try {
            return Paths.get(path);
        } catch (InvalidPathException e) {
            throw new IllegalArgumentException("invalid sandbox path: " + path, e);
        }
    }
}
