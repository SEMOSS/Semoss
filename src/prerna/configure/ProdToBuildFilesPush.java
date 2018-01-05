package prerna.configure;

import java.io.File;
import java.io.IOException;

import org.apache.commons.io.FileUtils;

public class ProdToBuildFilesPush {

	public static void main(String[] args) throws IOException {
		
		///////////////GET INPUTS/////////////////////////////////////////////
		
		// monolith classes location in BUILD
		String monolithBuildDir = "C:/Users/micstone/Desktop/SEMOSS_v3.1_x64/tomcat/webapps/Monolith/WEB-INF";
		
		// semossWeb location in BUILD
		String semossWebBuildDir = "C:/Users/micstone/Desktop/SEMOSS_v3.1_x64/tomcat/webapps";
		
		// monolith directories in PROD
		String monolithClassesProdDir = "C:/Users/micstone/workspace2/apache-tomcat-8.0.45/webapps/Monolith/WEB-INF/classes";
		String monolithLibProdDir = "C:/Users/micstone/workspace2/apache-tomcat-8.0.45/webapps/Monolith/WEB-INF/lib";
		String monolithWebProdDir = "C:/Users/micstone/workspace2/apache-tomcat-8.0.45/webapps/Monolith/WEB-INF/web.xml";
		
		// semossWeb location in PROD
		String semossWebProdDir = "C:/Users/micstone/workspace2/apache-tomcat-8.0.45/webapps/SemossWeb";
		
		/////////////////first, delete stuff in BUILD////////////////////////////
		
		// delete existing monolith folder components
		FileUtils.deleteDirectory(new File(monolithBuildDir + "/classes"));
		FileUtils.deleteDirectory(new File(monolithBuildDir + "/lib"));
		FileUtils.forceDelete(new File(monolithBuildDir + "/web.xml"));
		
		// delete existing semossWeb folder
		FileUtils.deleteDirectory(new File(semossWebBuildDir + "/SemossWeb"));
		
		/////////////////now, move the PROD files to BUILD///////////////////////////////
		
		//move monolith from prod to build
		FileUtils.copyDirectoryToDirectory(new File(monolithClassesProdDir), new File(monolithBuildDir));
		FileUtils.copyDirectoryToDirectory(new File(monolithLibProdDir), new File(monolithBuildDir));
		FileUtils.copyFileToDirectory(new File(monolithWebProdDir), new File(monolithBuildDir));
		
		// move semossWeb from prod to build
		FileUtils.copyDirectoryToDirectory(new File(semossWebProdDir), new File(semossWebBuildDir));
		
	}
	
}
