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

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

public class PythonVariableValidator {

  // list of python keywords
  private static final Set<String> PYTHON_KEYWORDS =
      new HashSet<>(
          Arrays.asList(
              "False",
              "None",
              "True",
              "and",
              "as",
              "assert",
              "async",
              "await",
              "break",
              "class",
              "continue",
              "def",
              "del",
              "elif",
              "else",
              "except",
              "finally",
              "for",
              "from",
              "global",
              "if",
              "import",
              "in",
              "is",
              "lambda",
              "nonlocal",
              "not",
              "or",
              "pass",
              "raise",
              "return",
              "try",
              "while",
              "with",
              "yield"));

  public static boolean isValidPythonVariableName(String name) {
    // Check if the name is null or empty
    if (name == null || name.isEmpty()) {
      return false;
    }

    // Check if the name is a Python keyword
    if (PYTHON_KEYWORDS.contains(name)) {
      return false;
    }

    // Check if the first character is a letter or underscore
    char firstChar = name.charAt(0);
    if (!Character.isLetter(firstChar) && firstChar != '_') {
      return false;
    }

    // Check if the rest of the characters are alphanumeric or underscores
    for (int i = 1; i < name.length(); i++) {
      char c = name.charAt(i);
      if (!Character.isLetterOrDigit(c) && c != '_') {
        return false;
      }
    }

    // If all checks pass, the name is valid
    return true;
  }
}
