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
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.nio.file.attribute.PosixFilePermission;
import java.util.EnumSet;
import java.util.Set;

/**
 * Writes a shell wrapper whose path is handed to an SDK as its {@code cliPath}.
 *
 * <p>The shell script is intentionally tiny and dispatches on the
 * {@link SandboxLauncherMain#ENV_BACKEND} env var so both {@link
 * LandlockLauncher} (Linux) and {@link SandboxExecLauncher} (macOS) can
 * use the same entry point - which matters because the Claude Agent SDK's
 * Python transport does {@code [cli_path, ...flags]} and has no hook to
 * prepend other arguments.
 *
 * <p>The script is written to the policy tmp dir (or the system tmp dir)
 * with mode 0700 and reused per-run - the parent process is free to
 * delete it once the spawn completes.
 */
public final class LauncherScriptWriter {

    private LauncherScriptWriter() {}

    /**
     * @param tmpDir          directory to write the script into; created if missing
     * @return absolute path to the generated, executable wrapper script
     */
    public static Path write(Path tmpDir) {
        Path dir = tmpDir != null ? tmpDir : Paths.get(System.getProperty("java.io.tmpdir"));
        try {
            Files.createDirectories(dir);
            Path script = Files.createTempFile(dir, "semoss-sandbox-", ".sh");
            script.toFile().deleteOnExit();
            Files.writeString(script, SCRIPT_BODY,
                    StandardOpenOption.WRITE, StandardOpenOption.TRUNCATE_EXISTING);
            try {
                Set<PosixFilePermission> perms = EnumSet.of(
                        PosixFilePermission.OWNER_READ,
                        PosixFilePermission.OWNER_WRITE,
                        PosixFilePermission.OWNER_EXECUTE);
                Files.setPosixFilePermissions(script, perms);
            } catch (UnsupportedOperationException ignored) {
                // non-POSIX FS - not our sandboxed platforms, ignore
            }
            return script.toAbsolutePath();
        } catch (IOException e) {
            throw new UncheckedIOException("failed to write sandbox wrapper script", e);
        }
    }

    /** Visible for testing. */
    static final String SCRIPT_BODY =
            "#!/bin/sh\n"
            + "# SEMOSS sandbox wrapper - generated, do not edit.\n"
            + "# Dispatches to the configured sandbox backend and execs the target binary.\n"
            + "set -e\n"
            + "case \"${SEMOSS_SANDBOX_BACKEND:-noop}\" in\n"
            + "  landlock)\n"
            + "    exec \"${SEMOSS_SANDBOX_JAVA_BIN:-java}\" -cp \"$SEMOSS_SANDBOX_CLASSPATH\" \\\n"
            + "         prerna.reactor.agent.sandbox.SandboxLauncherMain \"$@\"\n"
            + "    ;;\n"
            + "  sandbox-exec)\n"
            + "    exec /usr/bin/sandbox-exec -f \"$SEMOSS_SANDBOX_PROFILE_FILE\" \\\n"
            + "         \"$SEMOSS_SANDBOX_TARGET_CLI\" \"$@\"\n"
            + "    ;;\n"
            + "  noop|*)\n"
            + "    exec \"$SEMOSS_SANDBOX_TARGET_CLI\" \"$@\"\n"
            + "    ;;\n"
            + "esac\n";
}
