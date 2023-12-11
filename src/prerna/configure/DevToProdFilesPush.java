//package prerna.configure;
//
//import java.io.File;
//import java.io.IOException;
//
//import org.apache.commons.io.FileUtils;
//
//public class DevToProdFilesPush {
//
//	public static void main(String[] args) throws IOException {
//		
//		///////////////GET INPUTS/////////////////////////////////////////////
//		
//		//Semoss prod project folder
//		String semossProdDir = "C:/Users/micstone/workspaceProd/Semoss";
//		
//		//Semoss dev project folder
//		String semossDevDir = "C:/Users/micstone/workspace2/Semoss";
//		
//		//Monolith prod project folder
//		String monolithProdDir = "C:/Users/micstone/workspaceProd/Monolith";
//		
//		//Monolith dev project folder
//		String monolithDevDir = "C:/Users/micstone/workspace2/Monolith";
//		
//		//SemossWeb prod project folder
//		String semossWebProdDir = "C:/Users/micstone/workspaceProd/apache-tomcat-8.0.45/webapps";
//		
//		//SemossWeb dev project folder
//		String semossWebDevDir = "C:/Users/micstone/workspace2/apache-tomcat-8.0.45/webapps";
//		
//		/////////////////first, delete stuff in prod////////////////////////////
//		
//		// delete existing semoss prod src folder
//		File semossProdSrc = new File(semossProdDir + "/src");
//		FileUtils.deleteDirectory(semossProdSrc);
//		
//		// delete existing monolith prod src folder
//		File monolithProdSrc = new File(monolithProdDir + "/src");
//		FileUtils.deleteDirectory(monolithProdSrc);
//		
//		// delete existing semoss prod Solr/insightCore/conf/schema.xml
//		File semossProdSchema = new File(semossProdDir + "/Solr/insightCore/conf/schema.xml");
//		FileUtils.forceDelete(semossProdSchema);
//		
//		// delete existing semoss prod Solr/insightCore/conf/solrconfig.xml
//		File semossProdSolrconfig = new File(semossProdDir + "/Solr/insightCore/conf/solrconfig.xml");
//		FileUtils.forceDelete(semossProdSolrconfig);
//		
//		// delete existing semoss prod R folder
//		File semossProdR = new File(semossProdDir + "/R");
//		FileUtils.deleteDirectory(semossProdR);
//		
//		// delete existing semoss prod rpa folder
//		File semossProdRpa = new File(semossProdDir + "/rpa");
//		FileUtils.deleteDirectory(semossProdRpa);
//		
//		// delete existing semoss prod pom.xml folder
//		File semossProdPom = new File(semossProdDir + "/pom.xml");
//		FileUtils.forceDelete(semossProdPom);
//		
//		// delete existing monolith prod pom.xml folder
//		File monolithProdPom = new File(monolithProdDir + "/pom.xml");
//		FileUtils.forceDelete(monolithProdPom);
//		
//		// delete entire semoss web prod project
//		File semossProdWeb = new File(semossWebProdDir + "/SemossWeb");
//		FileUtils.forceDelete(semossProdWeb);
//		
//		/////////////////now, move the dev files///////////////////////////////
//		
//		//create file object for semoss prod and monolith prod directories
//		File semossProdDirFile = new File(semossProdDir);
//		File monolithProdDirFile = new File(monolithProdDir);
//		
//		// move entire semoss dev src directory to semoss prod src directory
//		File semossDevSrc = new File(semossDevDir + "/src");
//		FileUtils.copyDirectoryToDirectory(semossDevSrc, semossProdDirFile);
//		
//		// move entire monolith dev src directory to monolith prod src directory
//		File monolithDevSrc = new File(monolithDevDir + "/src");
//		FileUtils.copyDirectoryToDirectory(monolithDevSrc, monolithProdDirFile);
//		
//		// move semoss dev Solr/insightCore/conf/schema.xml to semoss prod
//		File semossDevSchema = new File(semossDevDir + "/Solr/insightCore/conf/schema.xml");
//		FileUtils.copyFileToDirectory(semossDevSchema, new File(semossProdDir + "/Solr/insightCore/conf"));
//		
//		// move semoss dev Solr/insightCore/conf/solrconfig.xml to semoss prod
//		File semossDevSolrconfig = new File(semossDevDir + "/Solr/insightCore/conf/solrconfig.xml");
//		FileUtils.copyFileToDirectory(semossDevSolrconfig, new File(semossProdDir + "/Solr/insightCore/conf"));
//		
//		// move entire semoss dev R folder to semoss prod
//		File semossDevR = new File(semossDevDir + "/R");
//		FileUtils.copyDirectoryToDirectory(semossDevR, semossProdDirFile);
//		
//		// move entire semoss dev rpa folder to semoss prod
//		File semossDevRpa = new File(semossDevDir + "/rpa");
//		FileUtils.copyDirectoryToDirectory(semossDevRpa, semossProdDirFile);
//		
//		// move semoss dev pom.xml to semoss prod
//		File semossDevPom = new File(semossDevDir + "/pom.xml");
//		FileUtils.copyFileToDirectory(semossDevPom, semossProdDirFile);
//		
//		// move monolith dev pom.xml to monolith prod
//		File monolithDevPom = new File(monolithDevDir + "/pom.xml");
//		FileUtils.copyFileToDirectory(monolithDevPom, monolithProdDirFile);
//		
//		// move entire semoss web dev project to semoss web prod folder
//		File semossWebDev = new File(semossWebDevDir + "/SemossWeb");
//		FileUtils.copyDirectoryToDirectory(semossWebDev, new File(semossWebProdDir));
//		
//	}
//}
