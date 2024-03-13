package prerna.junit.reactors.algorithm;

import org.junit.runner.RunWith;
import org.junit.runners.Suite;
import org.junit.runners.Suite.SuiteClasses;

import prerna.junit.reactors.algorithm.api.AdditionalDataTypesTest;
import prerna.junit.reactors.algorithm.api.DataFrameTypeEnumTest;
import prerna.junit.reactors.algorithm.api.SemossDataTypeTest;

@RunWith(Suite.class)
@SuiteClasses({ 
	AdditionalDataTypesTest.class,
	DataFrameTypeEnumTest.class,
	SemossDataTypeTest.class
	})
public class AlgorithmPackageTests {

}
