package prerna.engine.api;

import prerna.engine.impl.guardrail.DetoxifyGuardrailEngine;
import prerna.engine.impl.guardrail.GLiNERGuardrailEngine;

public enum GuardrailTypeEnum {

	EMBEDDED_DETOXIFY("EMBEDDED_DETOXIFY", GLiNERGuardrailEngine.class.getName()),
	EMBEDDED_GLINER("EMBEDDED_GLINER", DetoxifyGuardrailEngine.class.getName()),;

	private String guardrailName;
	private String guardrailClass;

	GuardrailTypeEnum(String guardrailName, String guardrailClass) {
		this.guardrailName = guardrailName;
		this.guardrailClass = guardrailClass;
	}

	/**
	 * 
	 * @return
	 */
	public String getGuardrailClass() {
		return this.guardrailClass;
	}

	/**
	 * 
	 * @return
	 */
	public String getGuardrailName() {
		return this.guardrailName;
	}

	/**
	 * 
	 * @param name
	 * @return
	 */
	public static GuardrailTypeEnum getEnumFromName(String name) {
		GuardrailTypeEnum[] allValues = values();
		for (GuardrailTypeEnum v : allValues) {
			if (v.getGuardrailName().equalsIgnoreCase(name)) {
				return v;
			}
		}
		throw new IllegalArgumentException("Invalid input for name " + name);
	}
}
