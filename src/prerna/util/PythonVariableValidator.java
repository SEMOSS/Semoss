package prerna.util;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

public class PythonVariableValidator {

	// list of python keywords
    private static final Set<String> PYTHON_KEYWORDS = new HashSet<>(Arrays.asList(
        "False", "None", "True", "and", "as", "assert", "async", "await", "break", "class",
        "continue", "def", "del", "elif", "else", "except", "finally", "for", "from", "global",
        "if", "import", "in", "is", "lambda", "nonlocal", "not", "or", "pass", "raise",
        "return", "try", "while", "with", "yield"
    ));

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
