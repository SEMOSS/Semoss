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
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.List;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.nounmeta.NounMetadata;

/**
 * Applies a multi-file patch in Codex's {@code apply_patch} envelope format.
 *
 * <p>Supports three operations per envelope: {@code Add File}, {@code Update File}, and
 * {@code Delete File}. The envelope begins with {@code *** Begin Patch} and ends with
 * {@code *** End Patch}. Examples:
 *
 * <pre>
 * *** Begin Patch
 * *** Add File: src/new/feature.ts
 * +export const greet = () =&gt; "hello";
 * +
 * *** Update File: src/existing.ts
 * @@
 *  some context
 * -old line
 * +new line
 *  more context
 * *** Delete File: src/old.ts
 * *** End Patch
 * </pre>
 *
 * <p>Context lines start with a single space, removed lines with {@code -}, added lines with
 * {@code +}. Multiple {@code @@} hunks per Update File block are supported and applied in order.
 * The patch is applied atomically — if any hunk fails to match exactly once, the whole patch is
 * rejected and no file changes are written.
 *
 * <p>This is the multi-file analogue of {@link MultiEditReactor}. Use this when you need to make
 * coordinated changes across several files; use MultiEdit for several edits in one file; use
 * EditFile for a single targeted edit.
 */
public class ApplyPatchReactor extends AbstractAgentToolReactor {

    public ApplyPatchReactor() {
        this.keysToGet   = new String[] { "patch" };
        this.keyRequired = new int[]    { 1       };
    }

    @Override
    protected NounMetadata doExecute() throws Exception {
        String patch = this.keyValue.get("patch");
        if (patch == null || patch.trim().isEmpty()) {
            return new NounMetadata("Error: patch is required", PixelDataType.CONST_STRING);
        }

        List<Op> ops;
        try {
            ops = parse(patch);
        } catch (IllegalArgumentException e) {
            return new NounMetadata("Error: " + e.getMessage(), PixelDataType.CONST_STRING);
        }
        if (ops.isEmpty()) {
            return new NounMetadata(
                    "Error: patch contained no operations",
                    PixelDataType.CONST_STRING);
        }

        // Two-phase apply: stage all updated contents in memory, then write.
        List<StagedWrite>  stagedWrites  = new ArrayList<>();
        List<File>         stagedDeletes = new ArrayList<>();

        for (int i = 0; i < ops.size(); i++) {
            Op op = ops.get(i);
            File target = resolveAndValidate(op.path);
            switch (op.kind) {
                case ADD: {
                    if (target.exists()) {
                        return new NounMetadata(
                                "Error: Add File target already exists: " + op.path,
                                PixelDataType.CONST_STRING);
                    }
                    stagedWrites.add(new StagedWrite(target, op.bodyForAdd()));
                    break;
                }
                case UPDATE: {
                    if (!target.exists() || !target.isFile()) {
                        return new NounMetadata(
                                "Error: Update File target not found: " + op.path,
                                PixelDataType.CONST_STRING);
                    }
                    String current = new String(Files.readAllBytes(target.toPath()),
                            StandardCharsets.UTF_8);
                    String updated;
                    try {
                        updated = applyHunks(current, op.hunks, op.path);
                    } catch (IllegalArgumentException e) {
                        return new NounMetadata("Error: " + e.getMessage(), PixelDataType.CONST_STRING);
                    }
                    stagedWrites.add(new StagedWrite(target, updated));
                    break;
                }
                case DELETE: {
                    if (!target.exists()) {
                        return new NounMetadata(
                                "Error: Delete File target not found: " + op.path,
                                PixelDataType.CONST_STRING);
                    }
                    if (target.isDirectory()) {
                        return new NounMetadata(
                                "Error: Delete File target is a directory: " + op.path,
                                PixelDataType.CONST_STRING);
                    }
                    stagedDeletes.add(target);
                    break;
                }
            }
        }

        // Commit phase
        for (StagedWrite sw : stagedWrites) {
            File parent = sw.file.getParentFile();
            if (parent != null && !parent.exists() && !parent.mkdirs()) {
                return new NounMetadata(
                        "Error: failed to create parent directories for: "
                                + toRelative(sw.file.getAbsolutePath()),
                        PixelDataType.CONST_STRING);
            }
            saveTextFileWithInsightAssetsBase64(sw.file, sw.content);
        }
        for (File del : stagedDeletes) {
            if (!del.delete()) {
                return new NounMetadata(
                        "Error: failed to delete: " + toRelative(del.getAbsolutePath()),
                        PixelDataType.CONST_STRING);
            }
        }

        StringBuilder summary = new StringBuilder();
        summary.append("Patch applied: ").append(ops.size()).append(" operation(s)\n");
        for (Op op : ops) {
            summary.append(" - ").append(op.kind.name().toLowerCase())
                   .append(": ").append(op.path).append('\n');
        }
        return new NounMetadata(summary.toString().trim(), PixelDataType.CONST_STRING);
    }

    // ----------------------------- parser -----------------------------

    private enum Kind { ADD, UPDATE, DELETE }

    private static final class Op {
        Kind kind;
        String path;
        List<String> addLines = new ArrayList<>();      // for ADD
        List<List<String>> hunks = new ArrayList<>();   // for UPDATE; each hunk is its raw lines

        String bodyForAdd() {
            StringBuilder sb = new StringBuilder();
            for (String l : addLines) {
                sb.append(l).append('\n');
            }
            return sb.toString();
        }
    }

    private static final class StagedWrite {
        final File file;
        final String content;
        StagedWrite(File file, String content) { this.file = file; this.content = content; }
    }

    private static List<Op> parse(String patch) {
        String[] lines = patch.split("\\r?\\n", -1);
        int i = 0;

        while (i < lines.length && lines[i].trim().isEmpty()) i++;
        if (i >= lines.length || !lines[i].startsWith("*** Begin Patch")) {
            throw new IllegalArgumentException(
                    "patch must start with '*** Begin Patch' (got: "
                            + (i < lines.length ? lines[i] : "<empty>") + ")");
        }
        i++;

        List<Op> ops = new ArrayList<>();
        Op current = null;
        List<String> currentHunk = null;

        for (; i < lines.length; i++) {
            String line = lines[i];
            if (line.startsWith("*** End Patch")) {
                if (current != null) {
                    if (currentHunk != null) {
                        current.hunks.add(currentHunk);
                        currentHunk = null;
                    }
                    ops.add(current);
                    current = null;
                }
                return ops;
            }
            if (line.startsWith("*** Add File:")) {
                flush(ops, current, currentHunk);
                current = new Op();
                current.kind = Kind.ADD;
                current.path = line.substring("*** Add File:".length()).trim();
                currentHunk = null;
                continue;
            }
            if (line.startsWith("*** Update File:")) {
                flush(ops, current, currentHunk);
                current = new Op();
                current.kind = Kind.UPDATE;
                current.path = line.substring("*** Update File:".length()).trim();
                currentHunk = null;
                continue;
            }
            if (line.startsWith("*** Delete File:")) {
                flush(ops, current, currentHunk);
                current = new Op();
                current.kind = Kind.DELETE;
                current.path = line.substring("*** Delete File:".length()).trim();
                ops.add(current);
                current = null;
                currentHunk = null;
                continue;
            }
            if (current == null) {
                // Skip stray blank line or noise outside of an operation
                if (line.trim().isEmpty()) continue;
                throw new IllegalArgumentException(
                        "unexpected content before any operation: " + line);
            }
            if (current.kind == Kind.ADD) {
                if (!line.startsWith("+")) {
                    if (line.trim().isEmpty()) continue;
                    throw new IllegalArgumentException(
                            "Add File body line must start with '+': " + line);
                }
                current.addLines.add(line.substring(1));
                continue;
            }
            if (current.kind == Kind.UPDATE) {
                if (line.startsWith("@@")) {
                    if (currentHunk != null) current.hunks.add(currentHunk);
                    currentHunk = new ArrayList<>();
                    continue;
                }
                if (currentHunk == null) {
                    // Tolerate an implicit single hunk that omits @@
                    currentHunk = new ArrayList<>();
                }
                currentHunk.add(line);
                continue;
            }
        }

        throw new IllegalArgumentException("patch is missing '*** End Patch'");
    }

    private static void flush(List<Op> ops, Op current, List<String> currentHunk) {
        if (current == null) return;
        if (currentHunk != null) current.hunks.add(currentHunk);
        ops.add(current);
    }

    // ----------------------------- apply -----------------------------

    private static String applyHunks(String original, List<List<String>> hunks, String path) {
        String[] origLines = original.split("\\n", -1);
        StringBuilder cursor = new StringBuilder();
        for (String l : origLines) cursor.append(l).append('\n');
        String working = cursor.toString();
        if (!original.endsWith("\n") && !original.isEmpty()) {
            working = working.substring(0, working.length() - 1); // mirror exact trailing newline
        } else if (original.isEmpty()) {
            working = "";
        }

        for (int hi = 0; hi < hunks.size(); hi++) {
            List<String> hunk = hunks.get(hi);
            StringBuilder before = new StringBuilder();
            StringBuilder after  = new StringBuilder();
            for (String l : hunk) {
                if (l.startsWith("+")) {
                    after.append(l.substring(1)).append('\n');
                } else if (l.startsWith("-")) {
                    before.append(l.substring(1)).append('\n');
                } else if (l.startsWith(" ")) {
                    before.append(l.substring(1)).append('\n');
                    after.append(l.substring(1)).append('\n');
                } else if (l.isEmpty()) {
                    before.append('\n');
                    after.append('\n');
                } else {
                    throw new IllegalArgumentException(
                            "hunk #" + (hi + 1) + " in " + path + " has invalid line: " + l);
                }
            }
            String beforeStr = before.toString();
            // Strip the trailing '\n' we appended unconditionally if the working text doesn't end in one
            if (!working.endsWith("\n") && beforeStr.endsWith("\n")) {
                beforeStr = beforeStr.substring(0, beforeStr.length() - 1);
            }
            String afterStr = after.toString();
            if (!working.endsWith("\n") && afterStr.endsWith("\n")) {
                afterStr = afterStr.substring(0, afterStr.length() - 1);
            }

            int occurrences = count(working, beforeStr);
            if (occurrences == 0) {
                throw new IllegalArgumentException(
                        "hunk #" + (hi + 1) + " in " + path + " did not match file contents");
            }
            if (occurrences > 1) {
                throw new IllegalArgumentException(
                        "hunk #" + (hi + 1) + " in " + path + " matched " + occurrences
                                + " locations — add more context to make it unique");
            }
            int idx = working.indexOf(beforeStr);
            working = working.substring(0, idx) + afterStr + working.substring(idx + beforeStr.length());
        }
        return working;
    }

    private static int count(String text, String target) {
        if (target.isEmpty()) return 0;
        int c = 0, i = 0;
        while ((i = text.indexOf(target, i)) != -1) { c++; i += target.length(); }
        return c;
    }

    @Override
    protected String getDescriptionForKey(String key) {
        if ("patch".equals(key)) {
            return "Multi-file patch in Codex apply_patch envelope format, starting with "
                 + "'*** Begin Patch' and ending with '*** End Patch'. Supports '*** Add File:', "
                 + "'*** Update File:' (with @@ hunks), and '*** Delete File:' operations.";
        }
        return super.getDescriptionForKey(key);
    }

    @Override
    public String getReactorDescription() {
        return "Applies a Codex-style apply_patch envelope spanning multiple files. Supports "
             + "Add/Update/Delete File operations. Atomic: any hunk that fails to match uniquely "
             + "rolls back the whole patch. Use MultiEdit for several edits in one file, EditFile "
             + "for a single edit.";
    }
}
