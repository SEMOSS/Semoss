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
import java.nio.file.Paths;

import prerna.reactor.agent.sandbox.linux.Landlock;

import com.sun.jna.Library;
import com.sun.jna.Native;
import com.sun.jna.StringArray;

/**
 * Child-JVM entry point invoked by {@link LandlockLauncher}.
 *
 * <p>Responsibility: read the policy JSON file whose path is in
 * {@link #ENV_POLICY_FILE}, apply landlock rules derived from it, then
 * replace this JVM with the real agent binary via {@code execvp(2)}.
 *
 * <p>Does nothing on non-Linux hosts — Mac uses {@link SandboxExecLauncher}
 * at a level above this, so this main should not be invoked on Mac.
 *
 * <p>Invocation convention (set by {@link LandlockLauncher#plan}):
 * <pre>
 *   java -cp &lt;semoss cp&gt; prerna.reactor.agent.sandbox.SandboxLauncherMain \
 *        &lt;target-binary&gt; [target-args...]
 *   env: SEMOSS_SANDBOX_POLICY_FILE=/path/to/policy.json
 * </pre>
 */
public final class SandboxLauncherMain {

    public static final String ENV_POLICY_FILE  = "SEMOSS_SANDBOX_POLICY_FILE";
    public static final String ENV_BACKEND      = "SEMOSS_SANDBOX_BACKEND";
    public static final String ENV_TARGET_CLI   = "SEMOSS_SANDBOX_TARGET_CLI";
    public static final String ENV_PROFILE_FILE = "SEMOSS_SANDBOX_PROFILE_FILE";

    /** Landlock ABI v1 filesystem rights. Newer rights must be gated by ABI. */
    private static final long HANDLED_FS_ABI_1 =
              Landlock.LANDLOCK_ACCESS_FS_EXECUTE
            | Landlock.LANDLOCK_ACCESS_FS_WRITE_FILE
            | Landlock.LANDLOCK_ACCESS_FS_READ_FILE
            | Landlock.LANDLOCK_ACCESS_FS_READ_DIR
            | Landlock.LANDLOCK_ACCESS_FS_REMOVE_DIR
            | Landlock.LANDLOCK_ACCESS_FS_REMOVE_FILE
            | Landlock.LANDLOCK_ACCESS_FS_MAKE_CHAR
            | Landlock.LANDLOCK_ACCESS_FS_MAKE_DIR
            | Landlock.LANDLOCK_ACCESS_FS_MAKE_REG
            | Landlock.LANDLOCK_ACCESS_FS_MAKE_SOCK
            | Landlock.LANDLOCK_ACCESS_FS_MAKE_FIFO
            | Landlock.LANDLOCK_ACCESS_FS_MAKE_BLOCK
            | Landlock.LANDLOCK_ACCESS_FS_MAKE_SYM;

    private static final long RO_BITS =
              Landlock.LANDLOCK_ACCESS_FS_EXECUTE
            | Landlock.LANDLOCK_ACCESS_FS_READ_FILE
            | Landlock.LANDLOCK_ACCESS_FS_READ_DIR;

    public interface LibCExec extends Library {
        LibCExec INSTANCE = Native.load("c", LibCExec.class);

        int execvp(String file, StringArray argv);

        String strerror(int errno);
    }

    public static void main(String[] args) {
        String target = System.getenv(ENV_TARGET_CLI);
        if (target == null || target.trim().isEmpty()) {
            System.err.println("SandboxLauncherMain: " + ENV_TARGET_CLI + " is not set");
            System.exit(64); // EX_USAGE
            return;
        }

        String policyPath = System.getenv(ENV_POLICY_FILE);
        if (policyPath == null || policyPath.trim().isEmpty()) {
            System.err.println("SandboxLauncherMain: " + ENV_POLICY_FILE + " is not set");
            System.exit(78); // EX_CONFIG
            return;
        }

        // Everything passed to us is the target's argv[1..]; put the target
        // binary path itself at argv[0] as exec(3) expects.
        String[] targetArgv = new String[args.length + 1];
        targetArgv[0] = target;
        System.arraycopy(args, 0, targetArgv, 1, args.length);

        SandboxPolicy policy;
        try {
            policy = PolicyJson.fromFile(Paths.get(policyPath));
        } catch (RuntimeException e) {
            System.err.println("SandboxLauncherMain: failed to read policy " + policyPath + ": " + e.getMessage());
            System.exit(78);
            return;
        }

        int rc = applyLandlock(policy);
        if (rc != 0) {
            if (policy.getEnforcement() == EnforcementMode.ENFORCE) {
                System.err.println("SandboxLauncherMain: landlock apply failed under ENFORCE — aborting spawn");
                System.exit(77); // EX_NOPERM
                return;
            }
            System.err.println("SandboxLauncherMain: landlock apply failed ("
                    + Landlock.errnoString() + "); continuing unsandboxed (enforcement="
                    + policy.getEnforcement() + ")");
        }

        // Hand off — execvp replaces this JVM with the target binary, preserving
        // stdio fds. Landlock rules are inherited across exec.
        int ev = LibCExec.INSTANCE.execvp(target, new StringArray(targetArgv));
        // If we get here, execvp failed.
        System.err.println("SandboxLauncherMain: execvp(" + target + ") failed rc=" + ev
                + " " + Landlock.errnoString());
        System.exit(127);
    }

    /**
     * @return 0 on success, non-zero on failure.
     */
    static int applyLandlock(SandboxPolicy policy) {
        if (policy.getEnforcement() == EnforcementMode.DISABLED) {
            return 0;
        }
        int abi = Landlock.probeAbiVersion();
        if (abi < 1) {
            return -1;
        }

        long handledFs = handledFsForAbi(abi);
        int rulesetFd = Landlock.createRuleset(handledFs);
        if (rulesetFd < 0) {
            return rulesetFd;
        }
        try {
            for (AllowedPath ap : policy.getAllowedPaths()) {
                if (addPathRule(rulesetFd, ap, handledFs) != 0) {
                    return -1;
                }
            }
            if (policy.getTmpDir().isPresent()
                    && addPathRule(rulesetFd, new AllowedPath(policy.getTmpDir().get(), AccessMode.RW), handledFs) != 0) {
                return -1;
            }

            if (Landlock.setNoNewPrivs() != 0) {
                return -1;
            }
            if (Landlock.restrictSelf(rulesetFd) != 0) {
                return -1;
            }
            return 0;
        } finally {
            Landlock.closeQuietly(rulesetFd);
        }
    }

    private static long handledFsForAbi(int abi) {
        long bits = HANDLED_FS_ABI_1;
        if (abi >= 2) {
            bits |= Landlock.LANDLOCK_ACCESS_FS_REFER;
        }
        if (abi >= 3) {
            bits |= Landlock.LANDLOCK_ACCESS_FS_TRUNCATE;
        }
        return bits;
    }

    private static int addPathRule(int rulesetFd, AllowedPath ap, long handledFs) {
        Path p = ap.getPath();
        int parentFd = Landlock.openPath(p);
        if (parentFd < 0) {
            // Missing directories are tolerated — they simply can't be accessed.
            // Log to stderr but don't fail the whole apply.
            System.err.println("SandboxLauncherMain: could not open " + p + " for landlock; skipping ("
                    + Landlock.errnoString() + ")");
            return 0;
        }
        try {
            long access = (ap.getMode() == AccessMode.RW ? handledFs : RO_BITS) & handledFs;
            int rc = Landlock.addPathBeneathRule(rulesetFd, access, parentFd);
            if (rc != 0) {
                System.err.println("SandboxLauncherMain: addPathBeneathRule(" + p + ") rc=" + rc + " "
                        + Landlock.errnoString());
                return rc;
            }
            return 0;
        } finally {
            Landlock.closeQuietly(parentFd);
        }
    }
}
