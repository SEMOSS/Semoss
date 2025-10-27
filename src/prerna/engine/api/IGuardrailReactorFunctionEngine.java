package prerna.engine.api;

import prerna.reactor.IReactor;
import prerna.sablecc2.om.GenRowStruct;
import prerna.sablecc2.om.NounStore;
import prerna.sablecc2.om.nounmeta.GuardrailNounMetadata;

/**
 * Interface for guardrail engines that combine reactor and function capabilities for content safety.
 * 
 * <p>This interface represents a specialized type of engine that functions as both a
 * {@link IReactor} for pipeline integration and an {@link IFunctionEngine} for
 * standalone execution. Guardrail engines are responsible for analyzing content
 * to ensure it meets safety, compliance, and quality standards.</p>
 * 
 * <p>Key capabilities include:</p>
 * <ul>
 *   <li><strong>Content Analysis:</strong> Analyze text, images, or other content for safety issues</li>
 *   <li><strong>Policy Enforcement:</strong> Apply organizational or platform policies</li>
 *   <li><strong>Risk Assessment:</strong> Identify and score potential risks in content</li>
 *   <li><strong>Pipeline Integration:</strong> Operate as part of data processing pipelines</li>
 *   <li><strong>Real-time Filtering:</strong> Provide immediate feedback on content appropriateness</li>
 * </ul>
 * 
 * <p>Common guardrail types include:</p>
 * <ul>
 *   <li><strong>Toxicity Detection:</strong> Identify harmful, offensive, or inappropriate content</li>
 *   <li><strong>PII Detection:</strong> Find personally identifiable information</li>
 *   <li><strong>Content Classification:</strong> Categorize content by type or sensitivity</li>
 *   <li><strong>Bias Detection:</strong> Identify potential bias or discrimination</li>
 * </ul>
 * 
 * @see {@link IReactor} for reactor pipeline capabilities
 * @see {@link IFunctionEngine} for function execution capabilities
 * @see {@link GuardrailTypeEnum} for available guardrail types
 * @see {@link GuardrailNounMetadata} for guardrail analysis results
 * @author SEMOSS
 */
public interface IGuardrailReactorFunctionEngine extends IReactor, IFunctionEngine {

	/** Configuration key for the guardrail type used in engine initialization */
	String GUARDRAIL_TYPE = "GUARDRAIL_TYPE";

	/**
	 * Gets the specific type of guardrail implemented by this engine.
	 * 
	 * <p>The guardrail type identifies the specific content analysis capabilities
	 * and algorithms used by this engine, such as toxicity detection, PII
	 * identification, or content classification.</p>
	 * 
	 * @return The {@link GuardrailTypeEnum} representing this engine's guardrail type
	 * @see {@link GuardrailTypeEnum} for available guardrail types
	 */
	GuardrailTypeEnum getGuardrailType();

	/**
	 * Executes guardrail analysis using reactor pipeline context.
	 * 
	 * <p>This method performs content analysis within the context of a reactor
	 * pipeline, using the provided noun store and row structure to access
	 * content and context information. The analysis results are returned as
	 * metadata that can be used by subsequent pipeline stages.</p>
	 * 
	 * @param ns The {@link NounStore} containing context and data for analysis
	 * @param curRow The {@link GenRowStruct} representing the current data row being processed
	 * @return {@link GuardrailNounMetadata} containing the analysis results and recommendations
	 * @see {@link NounStore} for context information
	 * @see {@link GenRowStruct} for row data structure
	 * @see {@link GuardrailNounMetadata} for analysis result format
	 */
	GuardrailNounMetadata execute(NounStore ns, GenRowStruct curRow);
}
