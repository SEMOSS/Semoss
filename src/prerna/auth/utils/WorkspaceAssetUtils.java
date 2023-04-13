package prerna.auth.utils;

import java.io.File;
import java.sql.SQLException;
import java.util.UUID;

import org.apache.commons.io.FileUtils;

import prerna.auth.AccessToken;
import prerna.auth.AuthProvider;
import prerna.auth.User;
import prerna.cluster.util.ClusterUtil;
import prerna.cluster.util.clients.AbstractCloudClient;
import prerna.ds.util.RdbmsQueryBuilder;
import prerna.engine.api.IRawSelectWrapper;
import prerna.engine.impl.SmssUtilities;
import prerna.project.api.IProject;
import prerna.project.impl.Project;
import prerna.query.querystruct.SelectQueryStruct;
import prerna.query.querystruct.filters.SimpleQueryFilter;
import prerna.query.querystruct.selectors.QueryColumnSelector;
import prerna.rdf.engine.wrappers.WrapperManager;
import prerna.util.AssetUtility;
import prerna.util.Constants;
import prerna.util.DIHelper;
import prerna.util.Utility;

public class WorkspaceAssetUtils extends AbstractSecurityUtils {
	
	public static final String WORKSPACE_APP_NAME = "Workspace";
	public static final String ASSET_APP_NAME = "Asset";
	public static final String HIDDEN_FILE = ".semoss";
	
	WorkspaceAssetUtils() {
		super();
	}
	
	
	//////////////////////////////////////////////////////////////////////
	// Creating workspace and asset metadata 
	//////////////////////////////////////////////////////////////////////
	
	/**
	 * Create the user workspace project for the provided access token
	 * @param token
	 * @return
	 * @throws Exception 
	 */
	public static String createUserWorkspaceProject(AccessToken token) throws Exception {
		String projectId = createEmptyProject(token, WORKSPACE_APP_NAME, false);
		registerUserWorkspaceProject(token, projectId);
		return projectId;
	}
	
	/**
	 * Create the user workspace project for the provided user and auth token
	 * @param user
	 * @param token
	 * @return
	 * @throws Exception 
	 */
	public static String createUserWorkspaceProject(User user, AuthProvider token) throws Exception {
		return createUserWorkspaceProject(user.getAccessToken(token));
	}
	
	/**
	 * Create the user asset project for the provided access token
	 * @param token
	 * @return
	 * @throws Exception 
	 */
	public static String createUserAssetProject(AccessToken token) throws Exception {
		String projectId = createEmptyProject(token, ASSET_APP_NAME, true);
		registerUserAssetProject(token, projectId);
		return projectId;
	}
	
	/**
	 * Create the user asset project for the provided user and auth token
	 * @param user
	 * @param token
	 * @return
	 * @throws Exception 
	 */
	public static String createUserAssetProject(User user, AuthProvider token) throws Exception {
		return createUserAssetProject(user.getAccessToken(token));
	}
	
	/**
	 * Generate 
	 * @param token
	 * @param projectName
	 * @param isAsset
	 * @return
	 * @throws Exception
	 */
	private static String createEmptyProject(AccessToken token, String projectName, boolean isAsset) throws Exception {
		// Create a new project id
		String projectId = UUID.randomUUID().toString();

		String userFolderLocation = AssetUtility.getUserAssetAndWorkspaceBaseFolder(projectName, projectId);
		File userFolder = new File(userFolderLocation);
		userFolder.mkdirs();

		// Add database into DIHelper so that the web watcher doesn't try to load as well
		File tempSmss = SmssUtilities.createTemporaryAssetAndWorkspaceSmss(projectId, projectName, isAsset, null);
		DIHelper.getInstance().setProjectProperty(projectId + "_" + Constants.STORE, tempSmss.getAbsolutePath());
		
		// Add the project to security db
		if(!isAsset) {
			SecurityUpdateUtils.addProject(projectId, false);
			SecurityUpdateUtils.addProjectOwner(projectId, token.getId());
		}
		
		// Create the project
		Project project = new Project();
		
		// Only at end do we add to DIHelper
		DIHelper.getInstance().setProjectProperty(projectId, project);
		String projects = (String) DIHelper.getInstance().getProjectProperty(Constants.PROJECTS);
		projects = projects + ";" + projectId;
		DIHelper.getInstance().setProjectProperty(Constants.PROJECTS, projects);
		
		// Rename .temp to .smss
		File smssFile = new File(tempSmss.getAbsolutePath().replace(".temp", ".smss"));
		FileUtils.copyFile(tempSmss, smssFile);
		tempSmss.delete();
		
		// Update engine smss file location
		project.openProject(smssFile.getAbsolutePath());
		
		if (ClusterUtil.IS_CLUSTER) {
			AbstractCloudClient.getClient().pushUserAssetOrWorkspace(projectId, isAsset);
		}
		
		DIHelper.getInstance().setProjectProperty(projectId + "_" + Constants.STORE, smssFile.getAbsolutePath());
		return projectId;
	}
	
	
	//////////////////////////////////////////////////////////////////////
	// Updating workspace and asset metadata 
	//////////////////////////////////////////////////////////////////////
	// TODO >>>timb: WORKSPACE - DONE - register workspace

	/**
	 * Register the user workspace project for the provided access token and project id
	 * @param token
	 * @param projectId
	 * @throws SQLException 
	 */
	public static void registerUserWorkspaceProject(AccessToken token, String projectId) throws SQLException {
		String[] colNames = new String[] {"TYPE", "USERID", "PROJECTID"};
		String[] types = new String[] {"varchar(255)", "varchar(255)", "varchar(255)"};
		String insertQuery = RdbmsQueryBuilder.makeInsert("WORKSPACEENGINE", colNames, types, 
				new String[] {	token.getProvider().name(), 
								token.getId(), 
								projectId});
		securityDb.insertData(insertQuery);
		securityDb.commit();
	}
	
	/**
	 * Register the user workspace project for the provided user, auth provider, and project id
	 * @param user
	 * @param provider
	 * @param projectId
	 * @throws SQLException 
	 */
	public static void registerUserWorkspaceProject(User user, AuthProvider provider, String projectId) throws SQLException {
		registerUserWorkspaceProject(user.getAccessToken(provider), projectId);
	}
	
	/**
	 * Register the user asset project for the provided access token and project id
	 * @param token
	 * @param projectId
	 * @throws SQLException 
	 */
	public static void registerUserAssetProject(AccessToken token, String projectId) throws SQLException {
		String[] colNames = new String[] {"TYPE", "USERID", "PROJECTID"};
		String[] types = new String[] {"varchar(255)", "varchar(255)", "varchar(255)"};
		String insertQuery = RdbmsQueryBuilder.makeInsert("ASSETENGINE", colNames, types, 
				new String[] {	token.getProvider().name(), 
								token.getId(), 
								projectId});
		securityDb.insertData(insertQuery);
		securityDb.commit();
	}
	
	/**
	 * Register the user asset project for the provided user, auth provider, and project id
	 * @param user
	 * @param provider
	 * @param projectId
	 * @throws SQLException 
	 */
	public static void registerUserAssetProject(User user, AuthProvider provider, String projectId) throws SQLException {
		registerUserAssetProject(user.getAccessToken(provider), projectId);
	}

	
	//////////////////////////////////////////////////////////////////////
	// Querying workspace and asset metadata 
	//////////////////////////////////////////////////////////////////////
	
	/**
	 * Get the user workspace project for the provided access token; returns null if there is none
	 * @param token
	 * @return
	 */
	public static String getUserWorkspaceProject(AccessToken token) {
//		String query = "SELECT PROJECTID FROM WORKSPACEENGINE WHERE "
//				+ "TYPE = '" + token.getProvider().name() + "' AND "
//				+ "USERID = '" + token.getId() + "'"
//				;
//		IRawSelectWrapper wrapper = WrapperManager.getInstance().getRawWrapper(securityDb, query);
		
		SelectQueryStruct qs = new SelectQueryStruct();
		qs.addSelector(new QueryColumnSelector("WORKSPACEENGINE__PROJECTID"));
		qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("WORKSPACEENGINE__TYPE", "==", token.getProvider().name()));
		qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("WORKSPACEENGINE__USERID", "==", token.getId()));
		IRawSelectWrapper wrapper = null;
		try {
			wrapper = WrapperManager.getInstance().getRawWrapper(securityDb, qs);
			if (wrapper.hasNext()) {
				 Object rs = wrapper.next().getValues()[0];
				 if (rs == null){
					 return null;
				 }
				return rs.toString();
			}
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			if(wrapper != null) {
				wrapper.cleanUp();
			}
		}
		
		return null;
	}
	
	/**
	 * Get the user workspace project for the provided user and auth provider; returns null if is there is none
	 * @param user
	 * @param provider
	 * @return
	 */
	public static String getUserWorkspaceProject(User user, AuthProvider provider) {
		return getUserWorkspaceProject(user.getAccessToken(provider));
	}
	
	/**
	 * Get the user asset project for the provided access token; returns null if there is none
	 * @param user
	 * @param token
	 * @return
	 */
	public static String getUserAssetProject(AccessToken token) {
//		String query = "SELECT PROJECTID FROM ASSETENGINE WHERE "
//				+ "TYPE = '" + token.getProvider().name() + "' AND "
//				+ "USERID = '" + token.getId() + "'"
//				;
//		IRawSelectWrapper wrapper = WrapperManager.getInstance().getRawWrapper(securityDb, query);
		
		SelectQueryStruct qs = new SelectQueryStruct();
		qs.addSelector(new QueryColumnSelector("ASSETENGINE__PROJECTID"));
		qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("ASSETENGINE__TYPE", "==", token.getProvider().name()));
		qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("ASSETENGINE__USERID", "==", token.getId()));
		IRawSelectWrapper wrapper = null;
		try {
			wrapper = WrapperManager.getInstance().getRawWrapper(securityDb, qs);
			if (wrapper.hasNext()) {
				 Object rs = wrapper.next().getValues()[0];
				 if (rs == null){
					 return null;
				 }
				return rs.toString();
				//return wrapper.next().getValues()[0].toString();
			}
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			if(wrapper != null) {
				wrapper.cleanUp();
			}
		}
		
		return null;
	}
	
	/**
	 * Get the user asset project for the provided user and auth provider; returns null if there is none
	 * @param user
	 * @param provider
	 * @return
	 */
	public static String getUserAssetProject(User user, AuthProvider provider) {
		return getUserAssetProject(user.getAccessToken(provider));
	}
	
	/**
	 * See if the project is a workspace or asset
	 * @param projectId
	 * @return
	 */
	public static boolean isAssetOrWorkspaceProject(String projectId) {
		SelectQueryStruct qs = new SelectQueryStruct();
		qs.addSelector(new QueryColumnSelector("WORKSPACEENGINE__PROJECTID"));
		qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("WORKSPACEENGINE__PROJECTID", "==", projectId));
		IRawSelectWrapper wrapper = null;
		try {
			wrapper = WrapperManager.getInstance().getRawWrapper(securityDb, qs);
			if (wrapper.hasNext()) {
				return true;
			} else {
				return isAssetProject(projectId);
			}
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			if(wrapper != null) {
				wrapper.cleanUp();
			}
		}
		
		return false;
	}
	
	/**
	 * Is the project an asset
	 * @param projectId
	 * @return
	 */
	public static boolean isAssetProject(String projectId) {
		SelectQueryStruct qs = new SelectQueryStruct();
		qs.addSelector(new QueryColumnSelector("ASSETENGINE__PROJECTID"));
		qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("ASSETENGINE__PROJECTID", "==", projectId));
		IRawSelectWrapper wrapper = null;
		try {
			wrapper = WrapperManager.getInstance().getRawWrapper(securityDb, qs);
			return wrapper.hasNext();
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			if(wrapper != null) {
				wrapper.cleanUp();
			}
		}

		return false;
	}
	
	
	//////////////////////////////////////////////////////////////////////
	// Asset folder locations
	//////////////////////////////////////////////////////////////////////
	public static String getUserAssetRootDirectory(User user, AuthProvider provider) {
		String assetProjectId = user.getAssetProjectId(provider);
		if (assetProjectId != null) {
			IProject assetProject = Utility.getProject(assetProjectId);
			if (assetProject != null) {
				String assetProjectName = assetProject.getProjectName();
				if (assetProjectName != null) {
					return AssetUtility.getProjectVersionFolder(assetProjectName, assetProjectId);
				}
			}
		}
		return null;
	}

}
