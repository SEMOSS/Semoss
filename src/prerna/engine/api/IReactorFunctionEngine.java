package prerna.engine.api;

import prerna.reactor.IReactor;

public interface IReactorFunctionEngine extends IReactor, IFunctionEngine {
	
	// things to think about
	// how do use this as a reactor - given the lifecycle portion of it
	// may be one of the pieces is the id to get to it
	// it is almost I have to go from reactor -- engine -- reactor (this is really just the engine)
	// may be that is the answer the first reactor is just a wrapper which ultimately just passes everything to this guy
	// by setting the nounstore
	// the initial one is purely just a pass through
	
}
