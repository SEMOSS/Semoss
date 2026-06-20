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

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.nio.file.attribute.PosixFilePermission;
import java.util.ArrayList;
import java.util.EnumSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.fasterxml.jackson.databind.ObjectMapper;

/**
 * Serialization contract between {@link SandboxPolicy} in the parent process
 * and {@link SandboxLauncherMain} in the launcher process.
 *
 * <p>JSON shape:
 * <pre>{@code
 * {
 *   "enforcement": "ENFORCE",
 *   "loopbackNetwork": true,
 *   "tmpDir": "/tmp/run-xyz",
 *   "paths": [
 *     {"path": "/opt/semosshome/room/abc", "mode": "RW"},
 *     {"path": "/etc/ssl/certs",           "mode": "RO"}
 *   ],
 *   "blocked": ["/home/user/.ssh", "/opt/semosshome"]
 * }
 * }</pre>
 */
public final class PolicyJson {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    private PolicyJson() {}

    /** Serialize to a compact JSON string suitable for an env var. */
    public static String toJson(SandboxPolicy policy) {
        Map<String, Object> root = toMap(policy);
        try {
            return MAPPER.writeValueAsString(root);
        } catch (IOException e) {
            throw new UncheckedIOException("failed to serialize SandboxPolicy", e);
        }
    }

    /**
     * Write the JSON to a fresh file readable only by the current user.
     *
     * @return the absolute path written to.
     */
    public static Path writeToFile(SandboxPolicy policy, Path targetDir, String filenamePrefix) {
        try {
            Files.createDirectories(targetDir);
            Path file = Files.createTempFile(targetDir, filenamePrefix, ".json");
            file.toFile().deleteOnExit();
            Files.writeString(file, toJson(policy),
                    StandardOpenOption.WRITE, StandardOpenOption.TRUNCATE_EXISTING);
            try {
                Set<PosixFilePermission> perms = EnumSet.of(
                        PosixFilePermission.OWNER_READ,
                        PosixFilePermission.OWNER_WRITE);
                Files.setPosixFilePermissions(file, perms);
            } catch (UnsupportedOperationException ignored) {
                // non-POSIX FS (e.g. Windows) - skip, not our target platform for sandboxing
            }
            return file.toAbsolutePath();
        } catch (IOException e) {
            throw new UncheckedIOException("failed to write policy JSON", e);
        }
    }

    /** Read back into a fresh {@link SandboxPolicy} - used by {@link SandboxLauncherMain}. */
    public static SandboxPolicy fromJson(String json) {
        try {
            @SuppressWarnings("unchecked")
            Map<String, Object> root = MAPPER.readValue(json, Map.class);
            return fromMap(root);
        } catch (IOException e) {
            throw new UncheckedIOException("failed to parse SandboxPolicy JSON", e);
        }
    }

    public static SandboxPolicy fromFile(Path file) {
        try {
            return fromJson(Files.readString(file));
        } catch (IOException e) {
            throw new UncheckedIOException("failed to read policy JSON at " + file, e);
        }
    }

    // helpers

    private static Map<String, Object> toMap(SandboxPolicy policy) {
        Map<String, Object> root = new LinkedHashMap<>();
        root.put("enforcement",   policy.getEnforcement().name());
        root.put("loopbackNetwork", policy.isLoopbackNetwork());
        policy.getTmpDir().ifPresent(p -> root.put("tmpDir", p.toString()));

        List<Map<String, String>> paths = new ArrayList<>(policy.getAllowedPaths().size());
        for (AllowedPath ap : policy.getAllowedPaths()) {
            Map<String, String> entry = new LinkedHashMap<>();
            entry.put("path", ap.getPath().toString());
            entry.put("mode", ap.getMode().name());
            paths.add(entry);
        }
        root.put("paths", paths);

        List<String> blocked = new ArrayList<>(policy.getBlockedPaths().size());
        for (Path p : policy.getBlockedPaths()) {
            blocked.add(p.toString());
        }
        root.put("blocked", blocked);
        return root;
    }

    @SuppressWarnings("unchecked")
    private static SandboxPolicy fromMap(Map<String, Object> root) {
        SandboxPolicyBuilder b = SandboxPolicy.builder();
        String enforcement = (String) root.getOrDefault("enforcement", EnforcementMode.ENFORCE.name());
        b.withEnforcement(EnforcementMode.valueOf(enforcement));
        b.withLoopbackNetwork((Boolean) root.getOrDefault("loopbackNetwork", Boolean.TRUE));

        Object tmpDir = root.get("tmpDir");
        if (tmpDir instanceof String) {
            b.withTmpDir(Path.of((String) tmpDir));
        }

        Object pathsObj = root.get("paths");
        if (pathsObj instanceof List) {
            for (Object entryObj : (List<Object>) pathsObj) {
                if (!(entryObj instanceof Map)) continue;
                Map<String, Object> entry = (Map<String, Object>) entryObj;
                String path = String.valueOf(entry.get("path"));
                String mode = String.valueOf(entry.get("mode"));
                if (AccessMode.RW.name().equals(mode)) {
                    b.withReadWrite(path);
                } else {
                    b.withRead(path);
                }
            }
        }

        Object blockedObj = root.get("blocked");
        if (blockedObj instanceof List) {
            for (Object p : (List<Object>) blockedObj) {
                if (p instanceof String) {
                    b.withBlock((String) p);
                }
            }
        }
        return b.build();
    }
}
