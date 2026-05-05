package prerna.reactor.engine;

import java.lang.reflect.Method;
import java.lang.reflect.Parameter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

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

	private static final Map<IEngine.CATALOG_TYPE, Class<?>> CATALOGUE_INTERFACE_MAP;

	private static final Set<String> BASE_INTERFACE_METHOD_NAMES;

	static {
		CATALOGUE_INTERFACE_MAP = new LinkedHashMap<>();
		CATALOGUE_INTERFACE_MAP.put(IEngine.CATALOG_TYPE.MODEL,    IModelEngine.class);
		CATALOGUE_INTERFACE_MAP.put(IEngine.CATALOG_TYPE.VECTOR,   IVectorDatabaseEngine.class);
		CATALOGUE_INTERFACE_MAP.put(IEngine.CATALOG_TYPE.STORAGE,  IStorageEngine.class);
		CATALOGUE_INTERFACE_MAP.put(IEngine.CATALOG_TYPE.FUNCTION, IFunctionEngine.class);
		CATALOGUE_INTERFACE_MAP.put(IEngine.CATALOG_TYPE.DATABASE, IDatabaseEngine.class);

		BASE_INTERFACE_METHOD_NAMES = Arrays.stream(IEngine.class.getMethods())
				.map(Method::getName)
				.collect(Collectors.toSet());
		// Also exclude close() from Closeable
		BASE_INTERFACE_METHOD_NAMES.add("close");
	}

	public GetEngineMethodsReactor() {
		this.keysToGet = new String[] { ReactorKeysEnum.ENGINE_TYPE.getKey() };
		this.keyRequired = new int[] { 1 };
	}

	@Override
	public NounMetadata execute() {
		organizeKeys();

		IEngine.CATALOG_TYPE catalogType = resolveRequestedType();
		Class<?> iface = CATALOGUE_INTERFACE_MAP.get(catalogType);
		List<Map<String, Object>> methods = extractMethods(iface);

		Map<String, Object> result = new LinkedHashMap<>();
		result.put("catalogueType", catalogType.name());
		result.put("methods", methods);

		return new NounMetadata(result, PixelDataType.CUSTOM_DATA_STRUCTURE);
	}

	/**
	 * Resolves the required catalogue type from the engineTypes key.
	 * Throws if the key is missing or the value is not a supported catalogue.
	 */
	private IEngine.CATALOG_TYPE resolveRequestedType() {
		String raw = this.keyValue.get(ReactorKeysEnum.ENGINE_TYPE.getKey());
		if (raw == null || raw.isBlank()) {
			throw new IllegalArgumentException(
					"Please provide a catalogue type using the engineTypes key. "
							+ "Valid values are: " + Arrays.toString(CATALOGUE_INTERFACE_MAP.keySet().toArray()));
		}
		raw = raw.trim().toUpperCase();
		try {
			IEngine.CATALOG_TYPE type = IEngine.CATALOG_TYPE.valueOf(raw);
			if (!CATALOGUE_INTERFACE_MAP.containsKey(type)) {
				throw new IllegalArgumentException(
						"Catalogue type '" + raw + "' is not supported. Supported types: "
								+ Arrays.toString(CATALOGUE_INTERFACE_MAP.keySet().toArray()));
			}
			return type;
		} catch (IllegalArgumentException e) {
			throw new IllegalArgumentException(
					"Unknown catalogue type '" + raw + "'. Valid values are: "
							+ Arrays.toString(CATALOGUE_INTERFACE_MAP.keySet().toArray()));
		}
	}

	private List<Map<String, Object>> extractMethods(Class<?> iface) {
		List<Map<String, Object>> methods = new ArrayList<>();

		for (Method method : iface.getMethods()) {
			// Skip methods that come from IEngine / Closeable
			if (BASE_INTERFACE_METHOD_NAMES.contains(method.getName())) {
				continue;
			}

			// Skip methods flagged with @IgnoreEngineLogging
			if (method.isAnnotationPresent(IgnoreEngineLogging.class)) {
				continue;
			}

			Map<String, Object> descriptor = new LinkedHashMap<>();
			descriptor.put("name", method.getName());
			descriptor.put("returnType", method.getReturnType().getSimpleName());
			descriptor.put("parameters", buildParameterList(method));

			methods.add(descriptor);
		}

		return methods;
	}

	/**
	 * Builds a list of parameter descriptors for the given method.
	 */
	private List<Map<String, String>> buildParameterList(Method method) {
		List<Map<String, String>> params = new ArrayList<>();
		for (Parameter param : method.getParameters()) {
			Map<String, String> p = new LinkedHashMap<>();
			p.put("name", param.getName());
			p.put("type", param.getType().getSimpleName());
			params.add(p);
		}
		return params;
	}

	@Override
	public String getReactorDescription() {
		return "Returns the list of callable methods for the specified engine catalogue.";
	}
}
