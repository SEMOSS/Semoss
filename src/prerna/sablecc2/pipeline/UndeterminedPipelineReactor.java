package prerna.sablecc2.pipeline;

import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.nounmeta.NounMetadata;

public class UndeterminedPipelineReactor extends AbstractReactor {

	@Override
	public NounMetadata execute() {
		throw new IllegalArgumentException("This reactor should never be called. "
				+ "It is just a palceholder for pipeline when trying to parse pixel statements "
				+ "without knowledge of frame reactors or insight specific reactors.");
	}

}