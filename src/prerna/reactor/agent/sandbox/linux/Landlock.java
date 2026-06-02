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
package prerna.reactor.agent.sandbox.linux;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

import com.sun.jna.Library;
import com.sun.jna.Native;
import com.sun.jna.Pointer;
import com.sun.jna.Structure;

/**
 * Minimal JNA binding for the Linux landlock LSM (kernel 5.13+).
 *
 * <p>Kept deliberately small: we call exactly four things - {@code prctl},
 * {@code syscall(SYS_landlock_create_ruleset)}, {@code syscall(SYS_landlock_add_rule)},
 * {@code syscall(SYS_landlock_restrict_self)} - and two helpers for {@code open(2)}
 * and {@code close(2)} on allowlist paths.
 *
 * <p>References:
 * <ul>
 *   <li>kernel docs: {@code Documentation/userspace-api/landlock.rst}
 *   <li>{@code linux/landlock.h}
 * </ul>
 */
public final class Landlock {

    private Landlock() {}

    // syscall numbers on x86_64 and aarch64 - same on both archs for these
    public static final int SYS_LANDLOCK_CREATE_RULESET = 444;
    public static final int SYS_LANDLOCK_ADD_RULE       = 445;
    public static final int SYS_LANDLOCK_RESTRICT_SELF  = 446;

    // prctl
    public static final int PR_SET_NO_NEW_PRIVS = 38;

    // landlock_rule_type
    public static final int LANDLOCK_RULE_PATH_BENEATH = 1;

    // Access rights (subset - enough for filesystem allowlist)
    // See linux/landlock.h
    public static final long LANDLOCK_ACCESS_FS_EXECUTE             = 1L << 0;
    public static final long LANDLOCK_ACCESS_FS_WRITE_FILE          = 1L << 1;
    public static final long LANDLOCK_ACCESS_FS_READ_FILE           = 1L << 2;
    public static final long LANDLOCK_ACCESS_FS_READ_DIR            = 1L << 3;
    public static final long LANDLOCK_ACCESS_FS_REMOVE_DIR          = 1L << 4;
    public static final long LANDLOCK_ACCESS_FS_REMOVE_FILE         = 1L << 5;
    public static final long LANDLOCK_ACCESS_FS_MAKE_CHAR           = 1L << 6;
    public static final long LANDLOCK_ACCESS_FS_MAKE_DIR            = 1L << 7;
    public static final long LANDLOCK_ACCESS_FS_MAKE_REG            = 1L << 8;
    public static final long LANDLOCK_ACCESS_FS_MAKE_SOCK           = 1L << 9;
    public static final long LANDLOCK_ACCESS_FS_MAKE_FIFO           = 1L << 10;
    public static final long LANDLOCK_ACCESS_FS_MAKE_BLOCK          = 1L << 11;
    public static final long LANDLOCK_ACCESS_FS_MAKE_SYM            = 1L << 12;
    public static final long LANDLOCK_ACCESS_FS_REFER               = 1L << 13;
    public static final long LANDLOCK_ACCESS_FS_TRUNCATE            = 1L << 14;
    public static final long LANDLOCK_ACCESS_FS_IOCTL_DEV           = 1L << 15;

    // open(2) flags
    public static final int O_PATH      = 010000000;
    public static final int O_CLOEXEC   = 02000000;

    /** JNA direct mapping for libc - just the handful of functions we need. */
    public interface LibC extends Library {
        LibC INSTANCE = Native.load("c", LibC.class);

        int syscall(int number, Object... args);

        int prctl(int option, long arg2, long arg3, long arg4, long arg5);

        int open(String path, int flags);

        int close(int fd);

        String strerror(int errno);
    }

    /**
     * {@code struct landlock_ruleset_attr} - single {@code __u64 handled_access_fs} field
     * in kernels we support. {@code handled_access_net} was added in 5.19 but is optional.
     */
    public static class RulesetAttr extends Structure {
        public long handled_access_fs;
        public long handled_access_net;    // present kernel >= 5.19; ignored earlier
        public long scoped;                // kernel >= 6.10; ignored earlier

        public RulesetAttr() {}

        public RulesetAttr(long handledAccessFs, long handledAccessNet, long scoped) {
            this.handled_access_fs  = handledAccessFs;
            this.handled_access_net = handledAccessNet;
            this.scoped             = scoped;
        }

        @Override
        protected java.util.List<String> getFieldOrder() {
            return java.util.Arrays.asList("handled_access_fs", "handled_access_net", "scoped");
        }
    }

    /**
     * {@code struct landlock_path_beneath_attr} - {@code __u64 allowed_access}
     * plus {@code __s32 parent_fd}.
     */
    public static class PathBeneathAttr extends Structure {
        public long allowed_access;
        public int  parent_fd;

        public PathBeneathAttr() {}

        public PathBeneathAttr(long allowedAccess, int parentFd) {
            this.allowed_access = allowedAccess;
            this.parent_fd      = parentFd;
        }

        @Override
        protected java.util.List<String> getFieldOrder() {
            return java.util.Arrays.asList("allowed_access", "parent_fd");
        }
    }

    /**
     * Probe support: returns the maximum supported landlock ABI version, or
     * -1 if the kernel does not implement landlock at all.
     *
     * <p>Also returns -1 early when we're not on Linux or when kernel < 5.13
     * judging by {@code /proc/sys/kernel/osrelease} - cheap filter that
     * avoids making a syscall that would crash the JVM on old kernels.
     */
    public static int probeAbiVersion() {
        if (!"linux".equalsIgnoreCase(System.getProperty("os.name"))) {
            return -1;
        }
        if (!meetsKernelFloor(5, 13)) {
            return -1;
        }
        try {
            int rc = LibC.INSTANCE.syscall(SYS_LANDLOCK_CREATE_RULESET, Pointer.NULL, 0L, 1 /* LANDLOCK_CREATE_RULESET_VERSION */);
            return rc >= 1 ? rc : -1;
        } catch (Throwable t) {
            return -1;
        }
    }

    static boolean meetsKernelFloor(int minMajor, int minMinor) {
        try {
            String osrelease = Files.readString(Paths.get("/proc/sys/kernel/osrelease")).trim();
            String[] parts = osrelease.split("[.-]");
            if (parts.length < 2) return false;
            int major = Integer.parseInt(parts[0]);
            int minor = Integer.parseInt(parts[1]);
            return major > minMajor || (major == minMajor && minor >= minMinor);
        } catch (IOException | NumberFormatException e) {
            return false;
        }
    }

    /**
     * Create a landlock ruleset file descriptor handling the widest set of
     * filesystem access rights supported by this kernel.
     *
     * @return the ruleset fd, or a negative errno on failure.
     */
    public static int createRuleset(long handledFs) {
        RulesetAttr attr = new RulesetAttr(handledFs, 0L, 0L);
        attr.write();
        return LibC.INSTANCE.syscall(SYS_LANDLOCK_CREATE_RULESET, attr.getPointer(), 24L /* sizeof */, 0);
    }

    /** Add a {@code path_beneath} rule to a ruleset. */
    public static int addPathBeneathRule(int rulesetFd, long allowedAccess, int parentFd) {
        PathBeneathAttr attr = new PathBeneathAttr(allowedAccess, parentFd);
        attr.write();
        return LibC.INSTANCE.syscall(
                SYS_LANDLOCK_ADD_RULE,
                rulesetFd,
                LANDLOCK_RULE_PATH_BENEATH,
                attr.getPointer(),
                0);
    }

    /**
     * Set {@code PR_SET_NO_NEW_PRIVS} - required before {@link #restrictSelf(int)}
     * for unprivileged callers.
     */
    public static int setNoNewPrivs() {
        return LibC.INSTANCE.prctl(PR_SET_NO_NEW_PRIVS, 1L, 0L, 0L, 0L);
    }

    /** Apply the ruleset to the current task and all descendants. */
    public static int restrictSelf(int rulesetFd) {
        return LibC.INSTANCE.syscall(SYS_LANDLOCK_RESTRICT_SELF, rulesetFd, 0);
    }

    /**
     * Open a path with {@code O_PATH|O_CLOEXEC} - the handle expected by
     * {@code landlock_add_rule} as {@code parent_fd}.
     *
     * @return open fd, or -1 on failure.
     */
    public static int openPath(Path path) {
        return LibC.INSTANCE.open(path.toString(), O_PATH | O_CLOEXEC);
    }

    public static void closeQuietly(int fd) {
        if (fd >= 0) {
            try {
                LibC.INSTANCE.close(fd);
            } catch (Throwable ignored) {
                // best effort
            }
        }
    }

    /** Last-known errno description, for logging. */
    public static String errnoString() {
        int errno = Native.getLastError();
        return "errno=" + errno + " (" + safeStrerror(errno) + ")";
    }

    private static String safeStrerror(int errno) {
        try {
            return LibC.INSTANCE.strerror(errno);
        } catch (Throwable t) {
            return "unknown";
        }
    }
}
