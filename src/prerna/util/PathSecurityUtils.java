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
package prerna.util;

import java.io.File;
import java.io.IOException;
import java.text.Normalizer;

/** Shared validation for identifiers and filesystem containment. */
public final class PathSecurityUtils {
	private PathSecurityUtils() {
	}

	/** Returns an unchanged identifier or name that cannot introduce path segments. */
	public static String requireSinglePathSegment(String value, String label) {
		if (value == null || value.isBlank() || value.equals(".") || value.equals("..")
				|| value.indexOf('/') >= 0 || value.indexOf('\\') >= 0 || value.indexOf(':') >= 0
				|| value.chars().anyMatch(Character::isISOControl)) {
			throw new IllegalArgumentException(label + " must be a single path segment");
		}
		// Existing path builders apply NFKC; reject separators introduced by that normalization too.
		String normalized = Normalizer.normalize(value, Normalizer.Form.NFKC);
		if (normalized.equals(".") || normalized.equals("..") || normalized.indexOf('/') >= 0
				|| normalized.indexOf('\\') >= 0 || normalized.indexOf(':') >= 0) {
			throw new IllegalArgumentException(label + " must be a single path segment");
		}
		return value;
	}

	/** Returns the canonical candidate only if its parent is the canonical root. */
	public static File requireDirectChild(File root, File candidate) throws IOException {
		File canonicalRoot = root.getCanonicalFile();
		File canonicalCandidate = candidate.getCanonicalFile();
		if (!canonicalRoot.equals(canonicalCandidate.getParentFile())) {
			throw new IllegalArgumentException("Path must be a direct child of the destination directory");
		}
		return canonicalCandidate;
	}

	/** Returns a canonical descendant; the root itself is not a valid target. */
	public static File requireDescendant(File root, File candidate) throws IOException {
		File canonicalRoot = root.getCanonicalFile();
		File canonicalCandidate = candidate.getCanonicalFile();
		if (canonicalCandidate.equals(canonicalRoot)
				|| !canonicalCandidate.toPath().startsWith(canonicalRoot.toPath())) {
			throw new IllegalArgumentException("Path must remain within the destination directory");
		}
		return canonicalCandidate;
	}
}
