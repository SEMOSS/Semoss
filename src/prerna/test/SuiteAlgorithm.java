package prerna.test;

import org.junit.runner.RunWith;
import org.junit.runners.Suite;
import org.junit.runners.Suite.SuiteClasses;

/**
*
* @author  August Bender
* @version 1.0
* @since   06-09-2015 
* Questions? Email abender@deloitte.com
*/

@RunWith(Suite.class)
@SuiteClasses({ ClusteringAlgorithmTest.class, WekaAprioriAlgorithTest.class })
public class SuiteAlgorithm {

/* 1) ClusteringAlgorithmTest
 * 	 a) Run
 * 	 b) Execute
 * 2) WekaAprioriAlgorithTest
 * 	 a) Execute
 *   b) generateDecisionRuleVizualization
 *	 c) generateDecisionRuleTable
 */
}
