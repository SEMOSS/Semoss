package prerna.sablecc2.reactor.expression;

import prerna.sablecc2.om.NounMetadata;

public class IfError extends OpBasic {

	@Override
	protected NounMetadata evaluate(Object[] values) {
		return this.nouns[0];
	}
}
