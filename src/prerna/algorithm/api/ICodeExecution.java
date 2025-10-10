package prerna.algorithm.api;

import prerna.om.Variable;

/**
 * Interface for objects that represent executable code with metadata about the execution context.
 * This interface provides methods to access information about code that has been or can be executed,
 * including the actual code content, programming language, and execution characteristics.
 * 
 * <p>
 * Implementations of this interface are used to encapsulate executable code along with its
 * metadata, enabling the system to understand and properly handle different types of code
 * execution scenarios.
 * </p>
 * 
 * @see {@link prerna.om.Variable.LANGUAGE} for supported programming languages
 */
public interface ICodeExecution {

	/**
	 * Returns the actual code that was or will be executed.
	 *
	 * @return The executable code as a string.
	 */
	String getExecutedCode();
	
	/**
	 * Returns the programming language of the executable code.
	 *
	 * @return The {@link prerna.om.Variable.LANGUAGE} enum value representing the programming language.
	 */
	Variable.LANGUAGE getLanguage();
	
	/**
	 * Determines whether this code execution represents user-provided script.
	 * User scripts are typically provided directly by end users, as opposed to
	 * system-generated or predefined code.
	 *
	 * @return True if this represents user-provided script, false otherwise.
	 */
	boolean isUserScript();
}
