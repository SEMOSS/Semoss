package prerna.reactor.export.postgres;

@Deprecated
public class ToPostgresCopyReactor extends PostgresCopyReactor {

	@Override
	public String getReactorDescription() {
		return "This reactor is deprecated. Please use PostgresCopy() instead.";
	}
}
