package prerna.engine.api;

import prerna.reactor.IReactor;
import prerna.sablecc2.om.GenRowStruct;
import prerna.sablecc2.om.NounStore;
import prerna.sablecc2.om.nounmeta.GuardrailNounMetadata;

public interface IGuardrailReactorFunctionEngine extends IReactor, IFunctionEngine {
	
	GuardrailNounMetadata execute(NounStore ns, GenRowStruct curRow);
}
