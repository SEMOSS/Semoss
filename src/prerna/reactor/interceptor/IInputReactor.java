package prerna.reactor.interceptor;

import prerna.reactor.IReactor;

/**
 * A marker interface for a reactor that intercepts the input to an IEngine method call.
 * The processing logic is implemented within the standard execute() method.
 */
public interface IInputReactor extends IReactor {
    // This is a marker interface. No methods are declared here.
}