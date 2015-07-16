package prerna.test;

import static org.junit.Assert.*;

import org.apache.log4j.PropertyConfigurator;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.Ignore;

import prerna.algorithm.learning.supervized.MatrixRegressionAlgorithm;
import prerna.algorithm.api.ITableDataFrame;
import prerna.ds.BTreeDataFrame;
import prerna.util.DIHelper;

/**
*
* @author  August Bender
* @version 1.0
* @since   06-18-2015 
* Questions? Email abender@deloitte.com
*/
public class MatrixRegressionAlgorithmTest {

	private static String workingDir = System.getProperty("user.dir");
	static int testCounter;
	
	private static MatrixRegressionAlgorithm alg;
	private static double[][] matrixPrime;
	private static double[] matrixSecunde;
	
	private static BendersTools bTools;
	private static BTreeDataFrame data;
	
	@BeforeClass 
	public static void setUpOnce(){
		bTools = new BendersTools();
		//Set the Sudo-Prop
		System.setProperty("file.separator", "/");
		String propFile = workingDir + "/RDF_Map.prop";
		DIHelper.getInstance().loadCoreProp(propFile);
		PropertyConfigurator.configure(workingDir + "/log4j.prop");
		
		//Initialize variables
		int complexity = 10;
		matrixPrime = new double[complexity][1];
		matrixSecunde = new double[complexity];
		
		//Set data values
		for(int x = 0; x < matrixPrime.length; x++){
			for(int y = 0; y < matrixPrime[x].length; y++){
				if(x%2 == 0){
					matrixPrime[x][y] = 0.0;
				} else {
					matrixPrime[x][y] = 1.0;
				}
			}
		}
		for(int x = 0; x < matrixSecunde.length; x++){
			if(x%2 == 0){
				matrixSecunde[x] = 0.0;
			} else {
				matrixSecunde[x] = 1.0;
			}
		}
		
		String[] headers = new String[]{"a", "b", "c", "d", "e", "f", "g", "h", "i", "j"};
		//data = bTools.createBTreeForMatrix(headers, matrixPrime, matrixSecunde);
		//TODO Set Matrix
		
		System.out.println("Test Started..");
	}
	
	@Before
	public void setUp(){
		testCounter++;
		System.out.println("Test " + testCounter + " starting..");
		alg = new MatrixRegressionAlgorithm();
		
	}
	
	@After
	public void tearDown(){
		System.out.println("Test " + testCounter + " ended..");
	}
	
	@AfterClass
	public static void finalTearDown(){
		System.out.println("Class Tear Down ...");
	}
	
	@Ignore
	@Test
	public void executeTest(){
		alg.runAlgorithm();
		 
		 //Coeff Array Data Asserts
		 for(int i = 0; i < alg.getCoeffArray().length; i++){
			 if(i%2 == 0){
				 assertTrue("Coeff Array Data..",(alg.getEstimateArray()[i] == -8.881784197001253E-17));
			 } else {
				 assertTrue("Coeff Array Data..",(alg.getEstimateArray()[i] == 1.0000000000000002));
			 }
		 }

		 //CoeffErrorsArray: ");
		 for(int i = 0; i < alg.getCoeffErrorsArray().length; i++){
			 if(i%2 == 0){
				 assertTrue("Coeff Errors Array Data..",(alg.getCoeffErrorsArray()[i] == 2.4157733292717553E-17 ));
			 } else {
				 assertTrue("Coeff Errors Array Data..",(alg.getCoeffErrorsArray()[i] == 3.41641940587532E-17));
			 }
		 }

		 //ResidualArray: ");
		 for(int i = 0; i < alg.getResidualArray().length; i++){
			 if(i%2 == 0){
				 assertTrue("Residual Array Data..",(alg.getResidualArray()[i] == 8.881784197001253E-17));
			 } else {
				 assertTrue("Residual Array Data..",(alg.getResidualArray()[i] == -2.220446049250313E-16));
			 }
		 }

		 //Estimated Array Data Asserts
		 for(int i = 0; i < alg.getEstimateArray().length; i++){
			 if(i%2 == 0){
				 assertTrue("Estimated Array Data..",(alg.getEstimateArray()[i] == -8.881784197001253E-17));
			 } else {
				 assertTrue("Estimated Array Data..",(alg.getEstimateArray()[i] == 1.0000000000000002));
			 }
			 
		 }

		 assertTrue("Estimated Array Data..",(alg.getStandardError() == 1.691041330490229E-16));
		
	}
	
}