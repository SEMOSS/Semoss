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
import java.io.InputStream;
import java.nio.file.Files;
import java.security.MessageDigest;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.nounmeta.NounMetadata;

/**
 * Computes a cryptographic hash of a file inside the working directory.
 *
 * <p>Useful for verifying downloads, detecting whether a file changed between turns, or comparing
 * two artifacts for equality without diffing their contents.
 *
 * <p>Default algorithm is SHA-256; pass {@code algorithm} to choose SHA-1, SHA-512, or MD5.
 * Returns the digest as a lowercase hex string along with the file size.
 */
public class HashFileReactor extends AbstractAgentToolReactor {

    private static final Set<String> ALLOWED_ALGORITHMS = new HashSet<>(Arrays.asList(
            "MD5", "SHA-1", "SHA-256", "SHA-512"));

    public HashFileReactor() {
        this.keysToGet   = new String[] { "path", "algorithm" };
        this.keyRequired = new int[]    { 1,      0           };
    }

    @Override
    protected NounMetadata doExecute() throws Exception {
        String path        = this.keyValue.get("path");
        String algorithmIn = this.keyValue.get("algorithm");
        if (path == null || path.trim().isEmpty()) {
            return new NounMetadata("Error: path is required", PixelDataType.CONST_STRING);
        }

        String algorithm = normalizeAlgorithm(algorithmIn);
        if (!ALLOWED_ALGORITHMS.contains(algorithm)) {
            return new NounMetadata(
                    "Error: algorithm must be one of MD5|SHA-1|SHA-256|SHA-512 (got: "
                            + algorithmIn + ")",
                    PixelDataType.CONST_STRING);
        }

        File file = resolveAndValidate(path);
        if (!file.exists() || !file.isFile()) {
            return new NounMetadata("Error: file not found: " + path, PixelDataType.CONST_STRING);
        }

        MessageDigest digest = MessageDigest.getInstance(algorithm);
        long total = 0;
        try (InputStream in = Files.newInputStream(file.toPath())) {
            byte[] buffer = new byte[8192];
            int read;
            while ((read = in.read(buffer)) != -1) {
                digest.update(buffer, 0, read);
                total += read;
            }
        }
        byte[] bytes = digest.digest();
        StringBuilder hex = new StringBuilder(bytes.length * 2);
        for (byte b : bytes) hex.append(String.format("%02x", b));

        StringBuilder sb = new StringBuilder();
        sb.append("path: ").append(toRelative(file.getAbsolutePath())).append('\n');
        sb.append("algorithm: ").append(algorithm).append('\n');
        sb.append("size_bytes: ").append(total).append('\n');
        sb.append(algorithm.toLowerCase().replace("-", "")).append(": ").append(hex);
        return new NounMetadata(sb.toString(), PixelDataType.CONST_STRING);
    }

    private static String normalizeAlgorithm(String in) {
        if (in == null || in.trim().isEmpty()) return "SHA-256";
        String upper = in.trim().toUpperCase();
        if (upper.equals("SHA1"))   return "SHA-1";
        if (upper.equals("SHA256")) return "SHA-256";
        if (upper.equals("SHA512")) return "SHA-512";
        return upper;
    }

    @Override
    protected String getDescriptionForKey(String key) {
        switch (key) {
            case "path":      return "Relative path of the file to hash (within the working directory).";
            case "algorithm": return "Hash algorithm: MD5, SHA-1, SHA-256 (default), or SHA-512.";
            default:          return super.getDescriptionForKey(key);
        }
    }

    @Override
    public String getReactorDescription() {
        return "Computes a cryptographic hash of a file. Default algorithm SHA-256; supports "
             + "MD5|SHA-1|SHA-256|SHA-512. Returns the digest as a lowercase hex string plus size.";
    }
}
