package prerna.test;

import org.junit.runner.RunWith;
import org.junit.runners.Suite;
import org.junit.runners.Suite.SuiteClasses;

/**
*
* @author  August Bender
* @version 1.0
* @since   06-19-2015 
* Questions? Email abender@deloitte.com
*/

@RunWith(Suite.class)
@SuiteClasses({ 
				ClusteringAlgorithmTest.class, 
				LocalOutlierFactorAlgorithmTest.class,
				PartitionedClusteringAlgorithmTest.class,
				WekaAprioriAlgorithTest.class,
				WekaClassificationTest.class,
				MatrixRegressionAlgorithmTest.class,
				SelfOrganizingMapTest.class
				})
public class SuiteAlgorithm {

/* 1) ClusteringAlgorithmTest
 * 	 a) Run
 * 	 b) Execute
 * 
 * 2) LocalOutlierFactorAlgorithmTest
 * 	 a) Execute_withListAndNames
 * 	 b) Execute_withMasterTableAndMasterNames
 * 
 * 2) PartitionedClusteringAlgorithm
 * 	 a) generateBaseClusterInformation
 * 	 b) generateInitialClusters
 *   c) execute
 *   
 * 3) WekaAprioriAlgorithTest
 * 	 a) Execute
 *   b) generateDecisionRuleVizualization
 *	 c) generateDecisionRuleTable
 *
 * 4) WekaClassificationTest
 * 	 a) Execute
 *   b) processTreeString
 *   
 * 5) MatrixRegressionAlgorithmTest
 * 	 a) Execute
 * 
 * 6) SelfOrganizingMapTest
 * 	 a) executeTest_withNoDataConstructor
 * 	 b) executeTest_withDataConstructor
 */
}
