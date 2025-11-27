package prerna.engine.api;

import prerna.reactor.IReactor;
import prerna.sablecc2.om.GenRowStruct;
import prerna.sablecc2.om.NounStore;
import prerna.sablecc2.om.nounmeta.GuardrailNounMetadata;

public interface IGuardrailReactorFunctionEngine extends IReactor, IFunctionEngine {

	// this is what the FE sends for the type of storage we are creating
	// as a result, cannot be a key in the smss file
	String GUARDRAIL_TYPE = "GUARDRAIL_TYPE";

	/**
	 * 
	 * @return
	 */
	GuardrailTypeEnum getGuardrailType();

	/**
	 * 
	 * @param ns
	 * @param curRow
	 * @return
	 */
	GuardrailNounMetadata execute(NounStore ns, GenRowStruct curRow);
}
