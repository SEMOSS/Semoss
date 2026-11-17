package prerna.reactor.security;

import java.util.Map;

import prerna.auth.User;
import prerna.auth.utils.SecurityCatalogDependencyUtils;
import prerna.reactor.AbstractReactor;
import prerna.reactor.ReactorDefinition;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.nounmeta.NounMetadata;

/**
 * Whether the user can view each engine and project an engine or project
 * depends on, all the way down. Reads the security database alone, so it works
 * on any server of a cluster.
 */
public class ValidateCatalogDependenciesReactor extends AbstractReactor {

	public ValidateCatalogDependenciesReactor() {
		this.keysToGet = new String[] { CatalogDependencyKeys.CATALOG_ID, CatalogDependencyKeys.CATALOG_TYPE };
		this.keyRequired = new int[] { 1, 0 };
	}

	@Override
	public NounMetadata execute() {
		organizeKeys();
		User user = this.insight.getUser();
		String catalogId = this.keyValue.get(CatalogDependencyKeys.CATALOG_ID);
		String catalogType = SecurityCatalogDependencyUtils.resolveCatalogType(catalogId,
				this.keyValue.get(CatalogDependencyKeys.CATALOG_TYPE));
		if (!SecurityCatalogDependencyUtils.userCanView(user, catalogId, catalogType)) {
			throw new IllegalArgumentException("The user does not have access to view " + catalogId);
		}
		Map<String, Object> canView = SecurityCatalogDependencyUtils.validateDependencies(user, catalogId);
		return new NounMetadata(canView, PixelDataType.MAP);
	}

	@Override
	public ReactorDefinition getReactorDefinition() {
		return super.getReactorDefinition()
				.description("Whether the user can view each engine and project an engine or project depends on")
				.key(CatalogDependencyKeys.CATALOG_ID, "The engine id or project id")
				.key(CatalogDependencyKeys.CATALOG_TYPE,
						"Its engine type or project type, such as FUNCTION or SKILL; read when not given");
	}

}
