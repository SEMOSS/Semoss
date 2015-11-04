package prerna.rdf.main;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.Properties;

import prerna.engine.api.IEngine;
import prerna.engine.api.ISelectStatement;
import prerna.engine.api.ISelectWrapper;
import prerna.rdf.engine.wrappers.WrapperManager;
import prerna.test.TestUtilityMethods;
import prerna.util.DIHelper;
import prerna.util.Utility;

public class TestLocalDB {

	public static void main(String[] args) throws IOException {
		TestUtilityMethods.loadDIHelper();
		System.out.println(DIHelper.getInstance().getLocalProp("algorithmAction"));
		System.out.println(DIHelper.getInstance().getLocalProp("algorithmTransformation"));

	}

}
