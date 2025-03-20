package prerna.engine.api;

import prerna.reactor.IReactor;
import prerna.sablecc2.om.nounmeta.GuardrailNounMetadata;

public interface IGuardrailReactorFunctionEngine extends IReactor, IFunctionEngine {
	
	GuardrailNounMetadata execute();
}
