//package prerna.configure;
//
//import java.io.File;
//import java.io.IOException;
//
//import org.apache.commons.io.FileUtils;
//
//public class ProdToBuildFilesPush {
//
//	public static void main(String[] args) throws IOException {
//		
//		///////////////GET INPUTS/////////////////////////////////////////////
//	
//		// monolith classes location in BUILD
//		String monolithBuildDir = "C:/Users/micstone/Desktop/SEMOSS_v3.1_x64/tomcat/webapps/Monolith/WEB-INF";
//		
//		//semosshome location in BUILD
//		String semosshomeBuildDir = "C:/Users/micstone/Desktop/SEMOSS_v3.1_x64/semosshome";
//		
//		// semossWeb location in BUILD
//		String semossWebBuildDir = "C:/Users/micstone/Desktop/SEMOSS_v3.1_x64/tomcat/webapps";
//		
//		// monolith directories in PROD
//		String monolithProdDir = "C:/Users/micstone/workspace2/apache-tomcat-8.0.45/webapps/Monolith";
//		
//		// semoss location in PROD
//		String semossProdDir = "C:/Users/micstone/workspace2/Semoss";
//		
//		// semossWeb location in PROD
//		String semossWebProdDir = "C:/Users/micstone/workspace2/apache-tomcat-8.0.45/webapps/SemossWeb";
//		
//		// location of r libraries to be added to build
//		String rLibFolder = "C:/Users/micstone/Desktop/library";
//		
//		/////////////////first, delete stuff in BUILD////////////////////////////
//		
//		// delete existing monolith folder components
//		FileUtils.deleteDirectory(new File(monolithBuildDir + "/classes"));
//		FileUtils.deleteDirectory(new File(monolithBuildDir + "/lib"));
//		FileUtils.forceDelete(new File(monolithBuildDir + "/web.xml"));
//		
//		//delete existing semosshome folder components
//		FileUtils.forceDelete(new File(semosshomeBuildDir + "/Solr/insightCore/conf/schema.xml"));
//		FileUtils.forceDelete(new File(semosshomeBuildDir + "/Solr/insightCore/conf/solrconfig.xml"));
//		FileUtils.deleteDirectory(new File(semosshomeBuildDir + "/rpa"));
//		FileUtils.deleteDirectory(new File(semosshomeBuildDir + "/R"));
//		FileUtils.deleteDirectory(new File(semosshomeBuildDir + "/portables/R-Portable/App/R-Portable/library"));
//		
//		// delete existing semossWeb folder
//		FileUtils.deleteDirectory(new File(semossWebBuildDir + "/SemossWeb"));
//		
//		/////////////////now, move the PROD files to BUILD///////////////////////////////
//		
//		//move monolith from prod to build
//		FileUtils.copyDirectoryToDirectory(new File(monolithProdDir + "/WEB-INF/classes"), new File(monolithBuildDir));
//		FileUtils.copyDirectoryToDirectory(new File(monolithProdDir + "/WEB-INF/lib"), new File(monolithBuildDir));
//		FileUtils.copyFileToDirectory(new File(monolithProdDir + "/WEB-INF/web.xml"), new File(monolithBuildDir));
//		
//		// move semosshome folder components from prod to build
//		FileUtils.copyFileToDirectory(new File(semossProdDir + "/Solr/insightCore/conf/schema.xml"), new File(semosshomeBuildDir + "/Solr/insightCore/conf"));
//		FileUtils.copyFileToDirectory(new File(semossProdDir + "/Solr/insightCore/conf/solrconfig.xml"), new File(semosshomeBuildDir + "/Solr/insightCore/conf"));
//		FileUtils.copyDirectoryToDirectory(new File(semossProdDir + "/rpa"), new File(semosshomeBuildDir));
//		FileUtils.copyDirectoryToDirectory(new File(semossProdDir + "/R"), new File(semosshomeBuildDir));
//		
//		// copy over libraries
//		FileUtils.copyDirectoryToDirectory(new File(rLibFolder), new File(semosshomeBuildDir + "/portables/R-Portable/App/R-Portable"));
//		
//		// move semossWeb from prod to build
//		FileUtils.copyDirectoryToDirectory(new File(semossWebProdDir), new File(semossWebBuildDir));
//	}
//	
//}
