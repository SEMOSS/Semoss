package prerna.reactor.engine;

import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import prerna.auth.User;
import prerna.auth.utils.AbstractSecurityUtils;
import prerna.auth.utils.SecurityEngineUtils;
import prerna.auth.utils.SecurityQueryUtils;
import prerna.engine.api.IDatabaseEngine;
import prerna.engine.api.IEngine;
import prerna.engine.api.IFunctionEngine;
import prerna.engine.api.IModelEngine;
import prerna.engine.api.IStorageEngine;
import prerna.engine.api.IVectorDatabaseEngine;
import prerna.logging.IgnoreEngineLogging;
import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.execptions.SemossPixelException;
import prerna.sablecc2.om.nounmeta.NounMetadata;

public class GetEngineMethodsReactor extends AbstractReactor {

	private static final Map<IEngine.CATALOG_TYPE, Class<?>> ENGINE_INTERFACE_MAP;
	private static final Set<String> BASE_INTERFACE_METHOD_NAMES;

	static {
		ENGINE_INTERFACE_MAP = new LinkedHashMap<>();
		ENGINE_INTERFACE_MAP.put(IEngine.CATALOG_TYPE.MODEL, IModelEngine.class);
		ENGINE_INTERFACE_MAP.put(IEngine.CATALOG_TYPE.VECTOR, IVectorDatabaseEngine.class);
		ENGINE_INTERFACE_MAP.put(IEngine.CATALOG_TYPE.STORAGE, IStorageEngine.class);
		ENGINE_INTERFACE_MAP.put(IEngine.CATALOG_TYPE.FUNCTION, IFunctionEngine.class);
		ENGINE_INTERFACE_MAP.put(IEngine.CATALOG_TYPE.DATABASE, IDatabaseEngine.class);

		BASE_INTERFACE_METHOD_NAMES = new HashSet<>();
		for (Method method : IEngine.class.getMethods()) {
			if (!method.isAnnotationPresent(IgnoreEngineLogging.class)) {
				BASE_INTERFACE_METHOD_NAMES.add(method.getName());
			}
		}
	}

	public GetEngineMethodsReactor() {
		this.keysToGet = new String[] { ReactorKeysEnum.ENGINE.getKey() };
		this.keyRequired = new int[] { 1 };
	}

	@Override
	public NounMetadata execute() {
		organizeKeys();

		User user = this.insight.getUser();
		if (user == null) {
			throw new SemossPixelException("User must be signed into an account in order to use this reactor");
		}
		if (AbstractSecurityUtils.anonymousUsersEnabled() && user.isAnonymous()) {
			throwAnonymousUserError();
		}

		String engineId = this.keyValue.get(ReactorKeysEnum.ENGINE.getKey());
		if (engineId == null || (engineId = engineId.trim()).isEmpty()) {
			throw new SemossPixelException("Must input an engine id");
		}

		engineId = SecurityQueryUtils.testUserEngineIdForAlias(user, engineId);

		if (!SecurityEngineUtils.userCanEditEngine(user, engineId)) {
			throw new SemossPixelException(
					"Engine '" + engineId + "' does not exist or the user does not have edit access.");
		}

		IEngine.CATALOG_TYPE engineType = SecurityEngineUtils.getEngineType(engineId);
		Class<?> iface = ENGINE_INTERFACE_MAP.get(engineType);
		List<Map<String, Object>> methods = extractMethods(iface);

		return new NounMetadata(methods, PixelDataType.CUSTOM_DATA_STRUCTURE);
	}

	private List<Map<String, Object>> extractMethods(Class<?> iface) {
		List<Map<String, Object>> methods = new ArrayList<>();
		for (Method method : iface.getMethods()) {
			if (BASE_INTERFACE_METHOD_NAMES.contains(method.getName())) {
				continue;
			}
			if (method.isAnnotationPresent(IgnoreEngineLogging.class)) {
				continue;
			}
			boolean isDeprecated = method.isAnnotationPresent(Deprecated.class);
			Map<String, Object> methodInfo = new LinkedHashMap<>();
			methodInfo.put("methodName", method.getName());
			methodInfo.put("deprecated", isDeprecated);
			methods.add(methodInfo);
		}
		return methods;
	}

	@Override
	public String getReactorDescription() {
		return "Returns the list of callable methods for the specified engine.";
	}

	@Override
	protected String getDescriptionForKey(String key) {
		if (key.equals(ReactorKeysEnum.ENGINE.getKey())) {
			return "The engine id";
		}
		return super.getDescriptionForKey(key);
	}
}
