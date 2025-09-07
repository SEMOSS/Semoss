package prerna.logging;

import org.apache.logging.log4j.core.LogEvent;
import org.apache.logging.log4j.core.config.plugins.Plugin;
import org.apache.logging.log4j.core.config.plugins.PluginFactory;
import org.apache.logging.log4j.core.filter.AbstractFilter;

@Plugin(name = "KafkaRabbitMQAvaliabilityFilter", category = "Core", elementType = "filter", printObject = true)
public class KafkaRabbitMQAvaliabilityFilter extends AbstractFilter {

	@Override
	public Result filter(LogEvent event) {
		if (!SemossLogUtils.isKafkaUp()) {
			return Result.DENY;
		} else {
			return Result.ACCEPT;
		}
	}

	@PluginFactory
	public static KafkaRabbitMQAvaliabilityFilter createFilter() {
		return new KafkaRabbitMQAvaliabilityFilter();
	}
}
