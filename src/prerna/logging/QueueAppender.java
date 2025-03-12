package prerna.logging;

import java.io.Serializable;
import java.util.regex.Pattern;
import org.apache.logging.log4j.core.Filter;
import org.apache.logging.log4j.core.Layout;
import org.apache.logging.log4j.core.LogEvent;
import org.apache.logging.log4j.core.appender.AbstractAppender;
import org.apache.logging.log4j.core.config.plugins.Plugin;
import org.apache.logging.log4j.core.config.plugins.PluginAttribute;
import org.apache.logging.log4j.core.config.plugins.PluginElement;
import org.apache.logging.log4j.core.config.plugins.PluginFactory;
import org.apache.logging.log4j.core.layout.PatternLayout;

@Plugin(
        name = "QueueAppender",
        category = "Core",
        elementType = "appender",
        printObject = true
)
public class QueueAppender extends AbstractAppender {
    //need to move this to a plugin
    private static final Pattern REQUESTID_PATTERN = Pattern.compile(".*requestId=([^\\],\\s]+).*");
    private final IQueueLogger queueLogger;

    protected QueueAppender(String name, Filter filter, Layout<? extends Serializable> layout, boolean ignoreExceptions, IQueueLogger queueLogger) {
        super(name, filter, layout, ignoreExceptions);
        this.queueLogger = queueLogger;
    }

    @PluginFactory
    public static QueueAppender createAppender(@PluginAttribute("name") String name, @PluginElement("Filter") Filter filter, @PluginElement("Layout") Layout<? extends Serializable> layout, @PluginAttribute("loggerClass") String loggerClass, @PluginAttribute("loggerConfig") String loggerConfig) {
        if (name == null) {
            LOGGER.error("No name provided for QueueAppender");
        }

        if (layout == null) {
            layout = PatternLayout.createDefaultLayout();
        }

        IQueueLogger iQueueLogger = null;

        try {
            Class<?> clazz = Class.forName(loggerClass);
            iQueueLogger = (IQueueLogger) clazz.getConstructor(String.class).newInstance(loggerConfig);
        } catch (Exception e) {
            LOGGER.error(e.getMessage());
        }

        return new QueueAppender(name, filter, layout, false, iQueueLogger);
    }

    public void append(LogEvent event) {
        byte[] bytes = this.getLayout().toByteArray(event);
        String message = new String(bytes);

        try {
            if (REQUESTID_PATTERN.matcher(message).find()) {
                System.out.println("Logged to Queue");
                this.queueLogger.send(message);
            } else {
                System.out.println(message);
            }
        } catch (Exception e) {
            LOGGER.error(e.getMessage());
        }

    }

    public void stop() {
        super.stop();
        if (this.queueLogger != null) {
            this.queueLogger.close();
        }

    }
}
