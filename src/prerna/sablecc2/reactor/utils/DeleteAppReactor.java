package prerna.sablecc2.reactor.utils;

import java.util.List;
import java.util.Vector;

import prerna.auth.SecurityUpdateUtils;
import prerna.engine.api.IEngine;
import prerna.nameserver.DeleteFromMasterDB;
import prerna.nameserver.utility.MasterDatabaseUtility;
import prerna.sablecc2.om.GenRowStruct;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.sablecc2.reactor.AbstractReactor;
import prerna.util.Constants;
import prerna.util.DIHelper;
import prerna.util.Utility;

public class DeleteAppReactor extends AbstractReactor {

	public DeleteAppReactor() {
		this.keysToGet = new String[] { ReactorKeysEnum.APP.getKey() };
	}

	@Override
	public NounMetadata execute() {
		// UserPermissionsMasterDB permissions = new UserPermissionsMasterDB();
		// ArrayList<String> ownedEngines = permissions
		// .getUserOwnedEngines(((User)
		// request.getSession().getAttribute(Constants.SESSION_USER)).getId());
		List<String> appIds = getAppIds();
		for (String appId : appIds) {
			IEngine engine = Utility.getEngine(appId);
			// we may have the alias
			if(engine == null) {
				// we may have the alias
				engine = Utility.getEngine(MasterDatabaseUtility.testEngineIdIfAlias(appId));
			}
			if (engine != null) {
				deleteEngine(engine);
			}

			// TODO session code
			// IEngine engine = getEngine(engineString, request);
			// if (this.securityEnabled) {
			// if (ownedEngines.contains(engineString)) {
			// deleteEngine(engine, request);
			// permissions.deleteEngine(
			// ((User)
			// request.getSession().getAttribute(Constants.SESSION_USER)).getId(),
			// engineString);
			// } else {
			//// return Response.status(400).entity("You do not have access
			// to
			// delete this database.").build();
			// return WebUtility.getResponse("You do not have access to
			// delete
			// this database.", 400);
			// }
			// } else {
			// deleteEngine(engine, request)
			// }
		}

		return new NounMetadata(true, PixelDataType.BOOLEAN, PixelOperationType.DELETE_ENGINE);
	}

	/**
	 * 
	 * @param coreEngine
	 * @return
	 */
	private boolean deleteEngine(IEngine coreEngine) {
		String engineId = coreEngine.getEngineId();
		coreEngine.deleteDB();

		// remove from dihelper... this is absurd
		String engineNames = (String) DIHelper.getInstance().getLocalProp(Constants.ENGINES);
		engineNames = engineNames.replace(";" + engineId, "");
		DIHelper.getInstance().setLocalProperty(Constants.ENGINES, engineNames);

		DeleteFromMasterDB remover = new DeleteFromMasterDB();
		remover.deleteEngineRDBMS(engineId);
		SecurityUpdateUtils.deleteApp(engineId);
		return true;
	}

	/**
	 * Get inputs
	 * @return list of engines to delete
	 */
	public List<String> getAppIds() {
		List<String> appIds = new Vector<String>();

		// see if added as key
		GenRowStruct grs = this.store.getNoun(this.keysToGet[0]);
		if (grs != null && !grs.isEmpty()) {
			int size = grs.size();
			for (int i = 0; i < size; i++) {
				appIds.add(grs.get(i).toString());
			}
			return appIds;
		}

		// no key is added, grab all inputs
		int size = this.curRow.size();
		for (int i = 0; i < size; i++) {
			appIds.add(this.curRow.get(i).toString());
		}
		return appIds;
	}


	//	// session code
	//	private boolean deleteEngine(IEngine coreEngine, HttpServletRequest request) {
	//		String engineName = coreEngine.getEngineId();
	//		coreEngine.deleteDB();
	//		// remove from session
	//		HttpSession session = request.getSession();
	//		ArrayList<Hashtable<String, String>> engines = (ArrayList<Hashtable<String, String>>) session
	//				.getAttribute(Constants.ENGINES);
	//		for (Hashtable<String, String> engine : engines) {
	//			String engName = engine.get("name");
	//			if (engName.equals(engineName)) {
	//				engines.remove(engine);
	//				System.out.println("Removed from engines");
	//				session.setAttribute(Constants.ENGINES, engines);
	//				break;//
	//			}
	//		}
	//		session.removeAttribute(engineName);
	//
	//		// remove from dihelper... this is absurd
	//		String engineNames = (String) DIHelper.getInstance().getLocalProp(Constants.ENGINES);
	//		engineNames = engineNames.replace(";" + engineName, "");
	//		DIHelper.getInstance().setLocalProperty(Constants.ENGINES, engineNames);
	//
	//		DeleteFromMasterDB remover = new DeleteFromMasterDB();
	//		remover.deleteEngineRDBMS(engineName);
	//
	//		SolrIndexEngine solrE;
	//		try {
	//			solrE = SolrIndexEngine.getInstance();
	//			if (solrE.serverActive()) {
	//				solrE.deleteEngine(engineName);
	//			}
	//		} catch (KeyManagementException e) {
	//			e.printStackTrace();
	//		} catch (NoSuchAlgorithmException e) {
	//			e.printStackTrace();
	//		} catch (KeyStoreException e) {
	//			e.printStackTrace();
	//		}
	//
	//		return true;
	//	}
	//
	//	// session code
	//	private AbstractEngine getEngine(String engineName, HttpServletRequest request) {
	//		HttpSession session = request.getSession();
	//		AbstractEngine engine = null;
	//		if (session.getAttribute(engineName) instanceof IEngine)
	//			engine = (AbstractEngine) session.getAttribute(engineName);
	//		else
	//			engine = (AbstractEngine) Utility.getEngine(engineName);
	//		return engine;
	//	}

}
