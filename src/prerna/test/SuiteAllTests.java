package prerna.test;

import org.junit.runner.RunWith;
import org.junit.runners.Suite;
import org.junit.runners.Suite.SuiteClasses;

/**
*
* @author  August Bender
* @version 1.0
* @since   05-29-2015 
* Questions? Email abender@deloitte.com
*/

@RunWith(Suite.class)
@SuiteClasses({ 
				SuiteQueries.class, 
				SuiteComponents.class, 
				SuiteAlgorithm.class
				})
public class SuiteAllTests {

/*
 * SuiteAllTests is a simple class to run suites of JUnit Tests.
 * A Suite of Suites to be precise. 
 */
}
