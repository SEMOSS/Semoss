package prerna.test;

import org.junit.runner.RunWith;
import org.junit.runners.Suite;
import org.junit.runners.Suite.SuiteClasses;

/**
* AutomationTest is a simple class to run suites of JUnit Tests
* 1) QueryTests
* 	a) Check all queries inside insights for all databases
* 2) ImportDataProcessorTest checks the 3 main Components of the ImportDataProcessor class
* 	a) Creating a new Database
* 	b) Adding to and existing Database
* 	c) Overriding/Replacing an existing Database
*
* @author  August Bender
* @version 1.0
* @since   03-24-2015 
* Questions? Email abender@deloitte.com
*/

@RunWith(Suite.class)
@SuiteClasses({ QueryTests.class, ImportDataProcessorTest.class})
public class AutomationTest {

}
