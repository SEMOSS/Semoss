package prerna.reactor.security;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import prerna.auth.User;
import prerna.auth.utils.CatalogTypes;
import prerna.auth.utils.SecurityCatalogDependencyUtils;
import prerna.auth.utils.SecurityCatalogDependencyUtils.Dependency;
import prerna.reactor.AbstractReactor;
import prerna.reactor.ReactorDefinition;
import prerna.sablecc2.om.GenRowStruct;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.nounmeta.NounMetadata;

/**
 * Replaces the dependencies of an engine or a project with the engines and
 * projects given. Each is checked against the security database alone, so this
 * works on any server of a cluster.
 */
public class SetCatalogDependenciesReactor extends AbstractReactor {

	public SetCatalogDependenciesReactor() {
		this.keysToGet = new String[] { CatalogDependencyKeys.CATALOG_ID, CatalogDependencyKeys.CATALOG_TYPE,
				CatalogDependencyKeys.DEPENDENCIES };
		this.keyRequired = new int[] { 1, 0, 0 };
	}

	@Override
	public NounMetadata execute() {
		organizeKeys();
		User user = this.insight.getUser();
		String catalogId = this.keyValue.get(CatalogDependencyKeys.CATALOG_ID);
		String catalogType = SecurityCatalogDependencyUtils.resolveCatalogType(catalogId,
				this.keyValue.get(CatalogDependencyKeys.CATALOG_TYPE));
		if (!SecurityCatalogDependencyUtils.userCanEdit(user, catalogId, catalogType)) {
			throw new IllegalArgumentException("The user does not have access to edit " + catalogId);
		}

		List<Dependency> dependencies = new ArrayList<>();
		for (Map<String, Object> entry : getDependencyMaps()) {
			Object id = entry.get("id");
			Object type = entry.get("type");
			if (!(id instanceof String) || !(type instanceof String)) {
				throw new IllegalArgumentException("Each dependency needs an id and a type");
			}
			String dependencyId = (String) id;
			if (dependencyId.equals(catalogId)) {
				throw new IllegalArgumentException("An engine or project cannot depend on itself");
			}
			String dependencyType = CatalogTypes.normalize((String) type);
			String stored = CatalogTypes.resolve(dependencyId);
			if (stored == null) {
				throw new IllegalArgumentException("No engine or project has the id '" + dependencyId + "'");
			}
			if (!stored.equals(dependencyType)) {
				throw new IllegalArgumentException(
						"'" + dependencyId + "' is a " + stored + ", not a " + dependencyType);
			}
			dependencies.add(new Dependency(dependencyId, dependencyType));
		}

		SecurityCatalogDependencyUtils.setDependencies(user, catalogId, catalogType, dependencies);

		NounMetadata noun = new NounMetadata(true, PixelDataType.BOOLEAN);
		noun.addAdditionalReturn(NounMetadata.getSuccessNounMessage("Successfully set the new dependencies"));
		return noun;
	}

	@Override
	public ReactorDefinition getReactorDefinition() {
		return super.getReactorDefinition()
				.description("Set the engines and projects an engine or project depends on")
				.key(CatalogDependencyKeys.CATALOG_ID, "The engine id or project id")
				.key(CatalogDependencyKeys.CATALOG_TYPE,
						"Its engine type or project type, such as FUNCTION or SKILL; read when not given")
				.key(CatalogDependencyKeys.DEPENDENCIES,
						"The engines and projects it depends on, each as {id, type} with its own engine type or "
								+ "project type. An empty list removes them all.");
	}

	@SuppressWarnings("unchecked")
	private List<Map<String, Object>> getDependencyMaps() {
		List<Map<String, Object>> dependencyList = new ArrayList<>();
		GenRowStruct grs = this.store.getGenRowStruct(CatalogDependencyKeys.DEPENDENCIES);
		if (grs != null && !grs.isEmpty()) {
			for (int i = 0; i < grs.size(); i++) {
				Object value = grs.get(i);
				if (!(value instanceof Map)) {
					throw new IllegalArgumentException("Each dependency must be a map of id and type");
				}
				dependencyList.add((Map<String, Object>) value);
			}
		}
		return dependencyList;
	}

}
