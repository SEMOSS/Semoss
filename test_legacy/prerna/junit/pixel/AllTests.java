package prerna.junit.pixel;

import org.junit.runner.RunWith;
import org.junit.runners.Suite;
import org.junit.runners.Suite.SuiteClasses;

import prerna.junit.reactors.UnitTests;
import prerna.util.gson.IHeadersDataRowAdapterTest;

@RunWith(Suite.class)
@SuiteClasses({ IHeadersDataRowAdapterTest.class, PixelUnitTests.class, UnitTests.class })
public class AllTests {
		
}