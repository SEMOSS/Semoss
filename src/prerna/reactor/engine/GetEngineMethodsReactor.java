package prerna.reactor.engine;

import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import prerna.engine.api.IDatabaseEngine;
import prerna.engine.api.IEngine;
import prerna.engine.api.IFunctionEngine;
import prerna.engine.api.IModelEngine;
import prerna.engine.api.IStorageEngine;
import prerna.engine.api.IVectorDatabaseEngine;
import prerna.logging.IgnoreEngineLogging;
import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;

public class GetEngineMethodsReactor extends AbstractReactor {

	private static final Map<IEngine.CATALOG_TYPE, Class<?>> ENGINE_INTERFACE_MAP;

	private static final Set<String> BASE_INTERFACE_METHOD_NAMES;

	static {
		ENGINE_INTERFACE_MAP = new LinkedHashMap<>();
		ENGINE_INTERFACE_MAP.put(IEngine.CATALOG_TYPE.MODEL,    IModelEngine.class);
		ENGINE_INTERFACE_MAP.put(IEngine.CATALOG_TYPE.VECTOR,   IVectorDatabaseEngine.class);
		ENGINE_INTERFACE_MAP.put(IEngine.CATALOG_TYPE.STORAGE,  IStorageEngine.class);
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
		this.keysToGet = new String[] { ReactorKeysEnum.ENGINE_TYPE.getKey() };
		this.keyRequired = new int[] { 1 };
	}

	@Override
	public NounMetadata execute() {
		organizeKeys();

		IEngine.CATALOG_TYPE engineType = resolveRequestedType();
		Class<?> iface = ENGINE_INTERFACE_MAP.get(engineType);
		List<Map<String, Object>> methods = extractMethods(iface);

		return new NounMetadata(methods, PixelDataType.CUSTOM_DATA_STRUCTURE);
	}

	/**
	 * Resolves the required engine type from the engineTypes key.
	 * Throws if the key is missing or the value is not a supported engine.
	 */
	private IEngine.CATALOG_TYPE resolveRequestedType() {
		String engineType = this.keyValue.get(ReactorKeysEnum.ENGINE_TYPE.getKey());
		if (engineType == null || engineType.isBlank()) {
			throw new IllegalArgumentException(
					"Please provide a engine type using the engineTypes key. "
							+ "Valid values are: " + Arrays.toString(ENGINE_INTERFACE_MAP.keySet().toArray()));
		}
		engineType = engineType.trim().toUpperCase();
		try {
			IEngine.CATALOG_TYPE type = IEngine.CATALOG_TYPE.valueOf(engineType);
			if (!ENGINE_INTERFACE_MAP.containsKey(type)) {
				throw new IllegalArgumentException(
						"Engine type '" + engineType + "' is not supported. Supported types: "
								+ Arrays.toString(ENGINE_INTERFACE_MAP.keySet().toArray()));
			}
			return type;
		} catch (IllegalArgumentException e) {
			throw new IllegalArgumentException(
					"Unknown engine type '" + engineType + "'. Valid values are: "
							+ Arrays.toString(ENGINE_INTERFACE_MAP.keySet().toArray()));
		}
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

			// Simplify parameter details to represent method overloading concisely
			List<String> parameterTypes = new ArrayList<>();
			for (Class<?> paramType : method.getParameterTypes()) {
				parameterTypes.add(paramType.getSimpleName());
			}
			methodInfo.put("parameters", parameterTypes);

			methods.add(methodInfo);
		}
		return methods;
	}

	@Override
	public String getReactorDescription() {
		return "Returns the list of callable methods for the specified engine.";
	}
}
