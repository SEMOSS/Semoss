package prerna.logger;

import org.apache.logging.log4j.ThreadContext;
import org.apache.logging.log4j.core.Filter;
import org.apache.logging.log4j.core.LogEvent;
import org.apache.logging.log4j.core.config.Node;
import org.apache.logging.log4j.core.config.plugins.Plugin;
import org.apache.logging.log4j.core.config.plugins.PluginAttribute;
import org.apache.logging.log4j.core.config.plugins.PluginFactory;
import org.apache.logging.log4j.core.filter.AbstractFilter;

import java.util.regex.Pattern;
@Plugin(name = "MDCRegexFilter", category = Node.CATEGORY, elementType = Filter.ELEMENT_TYPE, printObject = true)
public class MDCRegexFilter extends AbstractFilter {

    private final Pattern pattern;
    private final String key;


    public MDCRegexFilter(String key, String valRegex,  Result onMatch, Result onMismatch) {
        super(onMatch, onMismatch);
        this.key = key;
        this.pattern = Pattern.compile(valRegex);
    }

    @Override
    public Result filter(LogEvent event) {
        String value = ThreadContext.get(key);
        if(value != null && pattern.matcher(value).matches()){
            return onMatch;
        }
        return onMismatch;
    }


    @PluginFactory
    public static MDCRegexFilter createFilter(@PluginAttribute("valRegex") String regex, @PluginAttribute("key") String key ,
                                              @PluginAttribute("onMatch") Result onMatch,  @PluginAttribute("onMismatch") Result onMismatch) {
        return new MDCRegexFilter(key, regex, onMatch, onMismatch);
    }

}
