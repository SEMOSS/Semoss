//package prerna.util;
//
//import java.io.File;
//import java.io.IOException;
//
//import prerna.solr.SolrImportUtility;
//
//public class CSVInsightsWebWatcher extends AbstractFileWatcher{
//
//	@Override
//	public void loadFirst() {
//		File dir = new File(folderToWatch);
//		String[] fileNames = dir.list(this);
//		if(fileNames != null) {
//			for (int fileIdx = 0; fileIdx < fileNames.length; fileIdx++) {
//				try {
//					process(fileNames[fileIdx]);
//				} catch (RuntimeException ex) {
//					ex.printStackTrace();
//					logger.fatal("CSV Insight Failed " + folderToWatch + "/" + fileNames[fileIdx]);
//				}
//			}
//		}
//	}
//
//	@Override
//	public void process(String fileName) {
//		String solrInformationFile = folderToWatch + "\\" + fileName.substring(0, fileName.length()-3); // get rid of the .tg
//		if(solrInformationFile.endsWith("_META")) {
//			solrInformationFile = solrInformationFile.substring(0, solrInformationFile.length()-5);
//		}
//		solrInformationFile += "_Solr.txt";
//		try {
//			SolrImportUtility.processSolrTextDocument(solrInformationFile);
//		} catch (IOException e) {
//			classLogger.error(Constants.STACKTRACE, e);
//		}
//	}
//	
//	/**
//	 * 
//	 */
//	@Override
//	public void run() {
//		logger.info("Starting CSV Insights thread");
//		synchronized(monitor) {
//			loadFirst();
//			super.run();
//		}
//	}
//}
