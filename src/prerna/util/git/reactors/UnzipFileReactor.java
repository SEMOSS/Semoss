package prerna.util.git.reactors;

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;

import org.apache.tika.Tika;
import org.apache.tika.metadata.Metadata;

import prerna.auth.AuthProvider;
import prerna.auth.User;
import prerna.auth.utils.AbstractSecurityUtils;
import prerna.cluster.util.ClusterUtil;
import prerna.project.api.IProject;
import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.AssetUtility;
import prerna.util.Utility;
import prerna.util.ZipUtils;

public class UnzipFileReactor extends AbstractReactor {

	public UnzipFileReactor() {
		this.keysToGet = new String[] {ReactorKeysEnum.FILE_PATH.getKey(), ReactorKeysEnum.SPACE.getKey()};
		this.keyRequired = new int[] {1, 0};
	}

	@Override
	public NounMetadata execute() {
		organizeKeys();
		User user = this.insight.getUser();
		// check if user is logged in
		if (AbstractSecurityUtils.anonymousUsersEnabled() && user.isAnonymous()) {
			throwAnonymousUserError();
		}
		
		// specify the folder from the base
		String fileRelativePath = Utility.normalizePath(keyValue.get(keysToGet[0]));
		String space = this.keyValue.get(this.keysToGet[1]);
		
		// if security enables, you need proper permissions
		// this takes in the insight and does a user check that the user has access to perform the operations
		String baseFolder = AssetUtility.getAssetBasePath(this.insight, space, true);
		String zipFileLocation = (baseFolder + "/" + fileRelativePath).replace('\\', '/');
		File zipFile = new File(zipFileLocation);
		if(zipFile.exists() && !zipFile.isFile()) {
			throw new IllegalArgumentException("Cannot find zip file '" + fileRelativePath + "')");
		}

		try {	
			Tika tika = new Tika();
			FileInputStream inputstream = new FileInputStream(zipFile);
			String mimeType = tika.detect(inputstream, new Metadata());
			if (mimeType.equalsIgnoreCase("application/zip")) {
				//check if assets file is not uploaded, looks for the assets folder in the uploaded zip file and extract it.
				if(zipFile.getParent().contains("project")&& !zipFile.getName().equalsIgnoreCase("assets.zip")) {				
					ZipInputStream zipIn = new ZipInputStream(new FileInputStream(zipFileLocation));
					ZipEntry entry;
					Boolean assetsFound = false;
		            while ((entry = zipIn.getNextEntry()) != null) {
		            	if (entry.getName().startsWith("assets")) {
		            		assetsFound = true;
	                      extractFile(zipIn, entry, "assets",zipFile.getParent());
	                    }
	                    zipIn.closeEntry(); 
		            }	
		            zipIn.close();
		            if(!assetsFound) {
		            	throw new IllegalArgumentException("Assets folder not found in the zip file.");
		            }
				}else {
					ZipUtils.unzip(zipFileLocation, zipFile.getParent());
				}				
			}else {
				throw new IllegalArgumentException("Please upload the zip file.");
			}
			
		} catch (IOException | IllegalArgumentException e ) {
			throw new IllegalArgumentException("Unable to unzip file. Detailed error = " + e.getMessage());
		}
		
		if(ClusterUtil.IS_CLUSTER) {
			//is it in the user space?
			if (AssetUtility.USER_SPACE_KEY.equalsIgnoreCase(space)) {
				AuthProvider provider = user.getPrimaryLogin();
				String projectId = user.getAssetProjectId(provider);
				if(projectId!=null && !(projectId.isEmpty())) {
					ClusterUtil.pushUserWorkspace(projectId, true);
				}
			// is it in the insight space of a saved insight?
			} else if(space == null || space.trim().isEmpty() || space.equals(AssetUtility.INSIGHT_SPACE_KEY)) {
				if(this.insight.isSavedInsight()) {
					IProject project = Utility.getProject(this.insight.getProjectId());
					ClusterUtil.pushProjectFolder(project, zipFile.getParent());
				}
			// this is in the project space where space = project id
			} else {
				IProject project = Utility.getProject(space);
				ClusterUtil.pushProjectFolder(project, zipFile.getParent());
			}
		}
		
		return new NounMetadata(true, PixelDataType.BOOLEAN);
	}
	
	 private static void extractFile(ZipInputStream zipIn, ZipEntry entry, String folderToExtract,String outputDir) {
         File outputFile = new File(outputDir + entry.getName().substring(folderToExtract.length()));
         if (entry.isDirectory()) {
             outputFile.mkdirs(); 
         } else {            
             new File(outputFile.getParent()).mkdirs();
  
             try (BufferedOutputStream bos = new BufferedOutputStream(new FileOutputStream(outputFile))) {
                 byte[] buffer = new byte[1024];
                 int len;
                 while ((len = zipIn.read(buffer)) != -1) {
                     bos.write(buffer, 0, len);
                 }                
             } catch (IOException e) {
                 e.printStackTrace();
             }
         }
     }
	
}
