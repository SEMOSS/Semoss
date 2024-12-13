package prerna.reactor;

import java.io.File;
import java.util.Properties;
import java.util.zip.ZipOutputStream;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.eclipse.jgit.api.CloneCommand;
import org.eclipse.jgit.api.Git;
import org.eclipse.jgit.transport.UsernamePasswordCredentialsProvider;

import prerna.engine.impl.SmssUtilities;
import prerna.om.Insight;
import prerna.om.InsightStore;
import prerna.sablecc2.om.GenRowStruct;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.Constants;
import prerna.util.EngineUtility;
import prerna.util.Utility;
import prerna.util.ZipUtils;
import prerna.util.git.reactors.DeleteAssetReactor;
import prerna.util.git.reactors.UnzipFileReactor;
import prerna.reactor.insights.ReloadInsightClassesReactor;
import prerna.reactor.project.PublishProjectReactor;
import prerna.reactor.project.UploadProjectAppReactor;

public class PullAppCodeReactor extends AbstractReactor {
	
	private static final Logger classLogger = LogManager.getLogger(PullAppCodeReactor.class);
	
	private static final String FILE_SEPARATOR = java.nio.file.FileSystems.getDefault().getSeparator();	
	
	private static final String CLONE_ERROR = "Unable to clone ";	
	private static final String PATH = "version/assets/";
	private static final String ZIPEXT = ".zip";
	private static final String APP_ROOT = "app_root";
	private static final String ASSETS = "assets";
	
	public PullAppCodeReactor(){		
		this.keysToGet = new String[]{"repoUrl" , "token" , "projectBranch"};
		this.keyRequired = new int [] { 1 , 1, 1 };
	}

	@Override
	public NounMetadata execute() {
		organizeKeys();		
		String repoUrl = this.keyValue.get(this.keysToGet[0]);  
        String token = this.keyValue.get(this.keysToGet[1]);  
        String projectBranch = this.keyValue.get(this.keysToGet[2]); 	   
	    String clonedfileName = null;
        String zipFilePath = null;
        
	    try {
	    	
	    	String insightId = insight.getInsightId();
			Insight in = InsightStore.getInstance().get(insightId);
			File instanceDir = new File( Utility.normalizePath(in.getInsightFolder()));
			 
        	File localRepo = new File(instanceDir.toString()+FILE_SEPARATOR+projectBranch);
			if(!localRepo.exists()) { 
				localRepo.mkdirs(); 
			}
			 
            Git git = null;
        	File gitDir = new File(localRepo, ".git");
       	 	if (!gitDir.exists() && !gitDir.isDirectory()) {	                 
            	
           	 	 CloneCommand cloneCommand = Git.cloneRepository();
                 cloneCommand.setURI(repoUrl)
                              .setDirectory(localRepo)
                              .setBranch(projectBranch)
                              .setCredentialsProvider(new UsernamePasswordCredentialsProvider(token, ""));
                             
                 git = cloneCommand.call();
                 
                 File[] files = localRepo.listFiles();
                 
                 if (files != null && files.length != 0) { 
                         for (File file : files) {
                        	 clonedfileName = file.getName();                         
                         }
                 } else {
                	 classLogger.error(Constants.STACKTRACE, CLONE_ERROR + repoUrl);
                	 NounMetadata.getErrorNounMessage(CLONE_ERROR + repoUrl);
                 }
                 
                 File clonedfilePath =  new File(localRepo.toString()+FILE_SEPARATOR+clonedfileName);
                 String appId = null;
         		 String appName = null;
            	 File[] fileList = clonedfilePath.listFiles();
            	 for (File filePath : fileList) {
     				if (filePath.toString().endsWith(Constants.SEMOSS_EXTENSION)) {
     					//smssFileLoc = clonedfilePath +FILE_SEPARATOR + filePath;
     					Properties prop = Utility.loadProperties(filePath.toString());         					
         					appId = prop.getProperty(Constants.PROJECT); 
         					appName = prop.getProperty(Constants.PROJECT_ALIAS);
         				 break;	
         				}
            	 }
            	 File appPath = new File(EngineUtility.PROJECT_FOLDER +FILE_SEPARATOR+SmssUtilities.getUniqueName(appName,appId));
            	 if(appPath.exists()) {  
        	         
            		 String deletedAssets = deleteAssets(appId).toString();
            		 String unZippedFile = null;
            		 
            		 if(deletedAssets.equalsIgnoreCase("Success!")) {
            			 zipFilePath = appPath+FILE_SEPARATOR+APP_ROOT+ FILE_SEPARATOR +PATH;                 
                         File zipfiledir = new File(Utility.normalizePath(zipFilePath));
                         if(!zipfiledir.exists()) { 
                        	 zipfiledir.mkdirs();  
             			}               
                         ZipOutputStream zos = ZipUtils.zipFolder(clonedfilePath+FILE_SEPARATOR+ASSETS, 
                        		 zipfiledir + FILE_SEPARATOR + clonedfileName+ZIPEXT);    	         
                         zos.close();
                         
                         unZippedFile = unZipFile(clonedfileName,appId).toString();
            		 }
                     if(unZippedFile.equalsIgnoreCase("true")) {
                    	 reloadInsightClasses(appId);
                    	 publishProject(appId);
                     }
            	 }else {
            		 File zipFileName = new File(clonedfileName+ZIPEXT);            		 
            		 uploadProjectApp(zipFileName); 
            	 }
       	 	}
    	          
		}catch (Exception e) {
			classLogger.error(Constants.STACKTRACE, e);
			throw new IllegalArgumentException("Error occurred! "+ e.getMessage());
		}
    		
	    return NounMetadata.getSuccessNounMessage("Successfully pulled the app");
    	}
	
		private Object deleteAssets(String appId) {
			Object result = null;
			 try {
				 DeleteAssetReactor delReac = new DeleteAssetReactor();
		    	 GenRowStruct grs = new GenRowStruct();
		         grs.add(new NounMetadata(PATH,PixelDataType.FRAME));
		         this.getNounStore().addNoun(ReactorKeysEnum.FILE_PATH.getKey(), grs);
		         grs = new GenRowStruct();
		         grs.add(new NounMetadata(appId,PixelDataType.FRAME));
		         this.getNounStore().addNoun(ReactorKeysEnum.SPACE.getKey(), grs);
		         delReac.setNounStore(this.getNounStore()); 
		         delReac.setInsight(this.insight);
		         result = delReac.execute().getValue();
			 }catch (Exception e) {
				 classLogger.error(Constants.STACKTRACE, e);
				 throw new IllegalArgumentException("Error occurred! "+ e.getMessage());
			}
	         return result;
		}
		
		private Object unZipFile(String clonedfileName,String appId) {
			Object result = null;
			try {
				UnzipFileReactor unzip = new UnzipFileReactor();   
	            this.setNounStore(null);
	            GenRowStruct zipgrs = new GenRowStruct();
	            zipgrs.add(new NounMetadata(PATH+FILE_SEPARATOR +clonedfileName+ZIPEXT,PixelDataType.FRAME));
	            this.getNounStore().addNoun(ReactorKeysEnum.FILE_PATH.getKey(), zipgrs);
	            zipgrs = new GenRowStruct();
	            zipgrs.add(new NounMetadata(appId,PixelDataType.FRAME));
	            this.getNounStore().addNoun(ReactorKeysEnum.SPACE.getKey(), zipgrs);
	            unzip.setNounStore(this.getNounStore()); 
	            unzip.setInsight(this.insight);
	            result = unzip.execute().getValue();				
			}catch(Exception e) {
				 classLogger.error(Constants.STACKTRACE, e);
				 throw new IllegalArgumentException("Error occurred! "+ e.getMessage());
			} 
			return result;
		}
		
		private void reloadInsightClasses(String appId) {			
			try {
				ReloadInsightClassesReactor reload = new ReloadInsightClassesReactor();
                this.setNounStore(null);
                GenRowStruct reloadgrs = new GenRowStruct();                
                reloadgrs.add(new NounMetadata(appId,PixelDataType.FRAME));
                this.getNounStore().addNoun(ReactorKeysEnum.SPACE.getKey(), reloadgrs);
                reload.setNounStore(this.getNounStore()); 
                reload.setInsight(this.insight);
                reload.execute();
			}catch (Exception e) {
				classLogger.error(Constants.STACKTRACE, e);
				 throw new IllegalArgumentException("Error occurred! "+ e.getMessage());
			}		
		}
		private void publishProject(String appId) {			
			try {
				PublishProjectReactor publishReac = new PublishProjectReactor();
                this.setNounStore(null);
                GenRowStruct publgrs = new GenRowStruct();                
                publgrs.add(new NounMetadata(appId,PixelDataType.FRAME));
                this.getNounStore().addNoun(ReactorKeysEnum.PROJECT.getKey(), publgrs);
                publgrs = new GenRowStruct();
                publgrs.add(new NounMetadata("true",PixelDataType.FRAME));
                this.getNounStore().addNoun(ReactorKeysEnum.RELEASE.getKey(), publgrs);
                publishReac.setNounStore(this.getNounStore()); 
                publishReac.setInsight(this.insight);
                publishReac.execute();
			}catch (Exception e) {
				classLogger.error(Constants.STACKTRACE, e);
				throw new IllegalArgumentException("Error occurred! "+ e.getMessage());
			}		
		}
		
		private void uploadProjectApp(File zipFileName) {
			try {
				UploadProjectAppReactor reac = new UploadProjectAppReactor();
                
                GenRowStruct grs = new GenRowStruct();
                grs.add(new NounMetadata(zipFileName,PixelDataType.FRAME));
                this.getNounStore().addNoun(ReactorKeysEnum.FILE_PATH.getKey(), grs);
                
                grs = new GenRowStruct();
                grs.add(new NounMetadata("false",PixelDataType.CONST_STRING));
                this.getNounStore().addNoun(ReactorKeysEnum.GLOBAL.getKey(), grs);
                
                reac.setNounStore(this.getNounStore()); 
                reac.setInsight(this.insight);
                reac.execute();
			}catch (Exception e) {
				classLogger.error(Constants.STACKTRACE, e);
				throw new IllegalArgumentException("Error occurred! "+ e.getMessage());
			}
			
		}
    }
