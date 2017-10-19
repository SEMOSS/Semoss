package prerna.ds.r;

public class RregexValidator {

	public void Validate(String script) {
		// More can be added handled here once more incorrect R syntax is found.
		// If R script contains backslash, iterate over the user's input values,
		// if the value has a backslash, make sure the next value can be escaped.
		if (script.contains("\\")) {
			String[] inputs = script.split("\"");
			for (int i = 0; i < inputs.length; i++) {
				if (!((i & 1) == 0)) {
					String value = inputs[i];
					if (value.contains("\\")) {
						for (int j = 0; j < value.length(); j++) {
							char c = value.charAt(j);
							if (c == '\\') {
								if (!value.substring(j, j + 1).matches("\\|\'|\"|a|b|f|n|r|t|u|x|U")) {
									throw new IllegalArgumentException("Invalid Input!");
								}
							}

						}
					}
				}
			}
		}
	}
}
