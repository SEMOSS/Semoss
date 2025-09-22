package prerna.logging;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.logging.log4j.core.LogEvent;
import org.apache.logging.log4j.core.config.plugins.Plugin;
import org.apache.logging.log4j.core.config.plugins.PluginFactory;
import org.apache.logging.log4j.core.pattern.ConverterKeys;
import org.apache.logging.log4j.core.pattern.LogEventPatternConverter;
import org.apache.logging.log4j.core.pattern.PatternConverter;

import prerna.util.Constants;
@Plugin(name = "MaskMsg",category =PatternConverter.CATEGORY)
@ConverterKeys({"maskMsg"})
public class MaskingPatternConverter  extends LogEventPatternConverter{

	private final List<MaskRule> rules;
	
	public final static Map<String,String> maskPattern = Map.of(
            "(?i)(password\s*=\s*)[^, ]+","$1****",
            "(?i)(token\s*=\s*)[^, ]+","$1****",
            "(?i)(Session\s*=\s*)[^, ]+","$1****",
            "(?i)(ssn\s*=\s*)[0-9-]+","$1****"
    );
	
	private static class MaskRule {
		final Pattern regex;
		final String replacement;
		MaskRule(Pattern regex,String replacement){
			this.regex =regex;
			this.replacement =replacement;
		}
	}
	
	protected MaskingPatternConverter(List<MaskRule> rules) {
		super("maskMsg", "maskMsg");
		this.rules = rules;
	}
	
	@PluginFactory
	public static MaskingPatternConverter newInstance() {
		List<MaskRule> rules = new ArrayList<MaskRule>();
		
		Map<String, String> pattern = maskPattern;
		pattern.forEach((k, v) -> {
			Pattern regex = Pattern.compile(k);
			String replacement = v;
			rules.add(new MaskRule(regex, replacement));
		});
		 
		//String maskingKeys = "password,token,ssn,card,Session";//Utility.getDIHelperProperty(Settings.MASKING_KEYS);
		//keys = Arrays.stream(maskingKeys.split(",")).map(String::trim).filter(s -> !s.isEmpty()).collect(Collectors.toList());
		return new MaskingPatternConverter(rules);
	}

	@Override
	public void format(LogEvent event, StringBuilder toAppendTo) {
		
		String message = event.getMessage().getFormattedMessage();
		for(MaskRule rule : rules) {
			Matcher m = rule.regex.matcher(message);
			message = m.replaceAll(rule.replacement);
			//message = event.getMessage().getFormattedMessage().replaceAll("(?i)("+key+"=)[^, ]+", "$1****");
		}
		toAppendTo.append(message);
	}

}
