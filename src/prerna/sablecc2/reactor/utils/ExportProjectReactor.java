package prerna.sablecc2.reactor.utils;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Arrays;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.locks.Lock;
import java.util.zip.ZipOutputStream;

import org.apache.commons.io.FilenameUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

import prerna.auth.User;
import prerna.auth.utils.SecurityAdminUtils;
import prerna.auth.utils.SecurityProjectUtils;
import prerna.cluster.util.ClusterUtil;
import prerna.engine.impl.SmssUtilities;
import prerna.om.InsightFile;
import prerna.project.api.IProject;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.execptions.SemossPixelException;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.sablecc2.reactor.AbstractReactor;
import prerna.sablecc2.reactor.project.DownloadProjectInsightsReactor;
import prerna.util.Constants;
import prerna.util.DIHelper;
import prerna.util.ProjectSyncUtility;
import prerna.util.Utility;
import prerna.util.ZipUtils;

public class ExportProjectReactor extends AbstractReactor {

	private static final Logger classLogger = LogManager.getLogger(DownloadProjectInsightsReactor.class);

	private static final String CLASS_NAME = ExportProjectReactor.class.getName();

	public ExportProjectReactor() {
		this.keysToGet = new String[] { ReactorKeysEnum.PROJECT.getKey() };
	}

	@Override
	public NounMetadata execute() {
		Logger logger = getLogger(CLASS_NAME);
		logger.info("Checking database information and user permissions.");
		organizeKeys();
		String projectId = this.keyValue.get(this.keysToGet[0]);

		User user = this.insight.getUser();
		projectId = SecurityProjectUtils.testUserProjectIdForAlias(this.insight.getUser(), projectId);
		boolean isAdmin = SecurityAdminUtils.userIsAdmin(user);
		if (!isAdmin) {
			boolean isOwner = SecurityProjectUtils.userIsOwner(user, projectId);
			if (!isOwner) {
				throw new IllegalArgumentException("Project " + projectId + "does not exist or user does not have access to export.");
			}
		}

		logger.info("Exporting project now...");
		String baseFolder = DIHelper.getInstance().getProperty(Constants.BASE_FOLDER);
		IProject project = Utility.getProject(projectId);
		String projectName = project.getProjectName();
		String projectNameAndId = SmssUtilities.getUniqueName(projectName, projectId);
		String baseProjectDir = baseFolder + "/" + Constants.PROJECT_FOLDER;
		String thisProjectDir = baseProjectDir + "/" + projectNameAndId;

		String outputDir = this.insight.getInsightFolder();
		String zipFilePath = outputDir + "/" + projectNameAndId + "_project.zip";

		Lock lock = ProjectSyncUtility.getProjectLock(projectId);
		lock.lock();
		try {
			// zip project
			ZipOutputStream zos = null;
			try {
				DIHelper.getInstance().removeProjectProperty(projectId);
				logger.info("Stopping the project...");
				project.close();
				
				if(ClusterUtil.IS_CLUSTER) {
					logger.info("Creating insight database ...");
					File insightsFile = null;
					try {
						insightsFile = SecurityProjectUtils.createInsightsDatabase(projectId, outputDir);
					} catch (Exception e) {
						classLogger.error(Constants.STACKTRACE, e);
						throw new IllegalArgumentException("Error occurred attemping to generate the insights database for this project");
					}
					logger.info("Done creating insight database ...");

					// zip project folder minus insights
					logger.info("Zipping project files...");
					zos = ZipUtils.zipFolder(thisProjectDir, zipFilePath, 
							// ignore the current insights database
							Arrays.asList(projectNameAndId+"/"+FilenameUtils.getName(insightsFile.getAbsolutePath())));
					logger.info("Done zipping project files...");
					
					logger.info("Zipping insight database ...");
					ZipUtils.addToZipFile(insightsFile, zos, projectNameAndId);
					logger.info("Done zipping insight database...");
				} else {
					// zip project folder
					logger.info("Zipping project files...");
					zos = ZipUtils.zipFolder(thisProjectDir, zipFilePath);
					logger.info("Done zipping project files...");
				}
				
				// zip up the project metadata
				logger.info("Grabbing project metadata...");
				File projectMetaF = new File(outputDir+"/"+projectName+"_metadata.json");
				Map<String, Object> projectMeta = SecurityProjectUtils.getAggregateProjectMetadata(projectId, null, false);
				Gson gson = new GsonBuilder().setPrettyPrinting().create();
				FileWriter writer = null;
				try {
					writer = new FileWriter(projectMetaF);
					gson.toJson(projectMeta, writer);
				} finally {
					if(writer != null) {
						try {
							writer.close();
						} catch (IOException e) {
							classLogger.error(Constants.STACKTRACE, e);
						}
					}
				}
				logger.info("Zipping project metadata...");
				ZipUtils.addToZipFile(projectMetaF, zos, projectNameAndId);
				logger.info("Done zipping project metadata...");

				// add smss file
				logger.info("Zipping project smss...");
				File smss = new File(baseProjectDir + "/" + projectNameAndId + ".smss");
				ZipUtils.addToZipFile(smss, zos);
				logger.info("Done zipping project smss files...");
				logger.info("Zipping Complete");
			} catch (Exception e) {
				logger.info("Error occurred zipping up project");
				classLogger.error(Constants.STACKTRACE, e);
				throw new SemossPixelException("Error occurred generating zip file. Detailed message = " + e.getMessage());
			} finally {
				try {
					if (zos != null) {
						zos.flush();
						zos.close();
					}
				} catch (IOException e) {
					classLogger.error(Constants.STACKTRACE, e);
				}
			}
		} finally {
			// open it back up
			logger.info("Opening the project again ... ");
			Utility.getProject(projectId);
			lock.unlock();
		}

		// Generate a new key for the name of the zip file.
		String downloadKey = UUID.randomUUID().toString();
		InsightFile insightFile = new InsightFile();
		insightFile.setFileKey(downloadKey);
		insightFile.setDeleteOnInsightClose(true);
		insightFile.setFilePath(zipFilePath);
		this.insight.addExportFile(downloadKey, insightFile);
		return new NounMetadata(downloadKey, PixelDataType.CONST_STRING, PixelOperationType.FILE_DOWNLOAD);
	}

}
