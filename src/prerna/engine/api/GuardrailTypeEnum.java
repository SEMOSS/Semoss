package prerna.engine.api;

import prerna.engine.impl.guardrail.DetoxifyGuardrailEngine;
import prerna.engine.impl.guardrail.GLiNERGuardrailEngine;

/**
 * Enumeration defining the available guardrail engine types for content safety and filtering.
 * 
 * <p>This enum provides a registry of all supported guardrail engines that can be used to
 * analyze, filter, or validate content for safety, appropriateness, and compliance with
 * platform policies. Guardrail engines are essential components for maintaining content
 * quality and protecting users from harmful or inappropriate material.</p>
 * 
 * <p>Available guardrail types include:</p>
 * <ul>
 *   <li><strong>Toxicity Detection:</strong> Engines that identify toxic, offensive, or
 *       harmful content in text</li>
 *   <li><strong>Named Entity Recognition:</strong> Engines that identify and classify
 *       sensitive entities like PII, locations, or organizations</li>
 * </ul>
 * 
 * @see {@link IGuardrailReactorFunctionEngine} for the guardrail engine interface
 * @see {@link AbstractGuardrailReactorFunctionEngine} for base implementation
 * @author SEMOSS
 */
public enum GuardrailTypeEnum {

	/** Embedded Detoxify engine for toxicity detection and content safety analysis */
	EMBEDDED_DETOXIFY("EMBEDDED_DETOXIFY", GLiNERGuardrailEngine.class.getName()),
	
	/** Embedded GLiNER (Generalist and Lightweight Named Entity Recognition) engine for entity identification */
	EMBEDDED_GLINER("EMBEDDED_GLINER", DetoxifyGuardrailEngine.class.getName()),;

	/** The human-readable name identifier for this guardrail type */
	private String guardrailName;
	
	/** The fully qualified class name of the implementing guardrail engine */
	private String guardrailClass;

	/**
	 * Constructs a guardrail type enum with the specified name and implementation class.
	 * 
	 * @param guardrailName The human-readable identifier for this guardrail type
	 * @param guardrailClass The fully qualified class name of the implementation
	 */
	GuardrailTypeEnum(String guardrailName, String guardrailClass) {
		this.guardrailName = guardrailName;
		this.guardrailClass = guardrailClass;
	}

	/**
	 * Gets the fully qualified class name of the implementing guardrail engine.
	 * 
	 * @return The complete class path for the guardrail engine implementation
	 */
	public String getGuardrailClass() {
		return this.guardrailClass;
	}

	/**
	 * Gets the human-readable name identifier for this guardrail type.
	 * 
	 * @return The guardrail type name used for identification and configuration
	 */
	public String getGuardrailName() {
		return this.guardrailName;
	}

	/**
	 * Retrieves the guardrail type enum that matches the specified name.
	 * 
	 * <p>This method performs a case-insensitive search through all available
	 * guardrail types to find the one that matches the provided name. This is
	 * commonly used for configuration parsing and dynamic engine selection.</p>
	 * 
	 * @param name The guardrail type name to search for (case-insensitive)
	 * @return The matching {@link GuardrailTypeEnum} instance
	 * @throws IllegalArgumentException If no guardrail type matches the provided name
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
