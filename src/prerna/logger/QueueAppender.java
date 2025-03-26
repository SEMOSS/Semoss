package prerna.logger;

import org.apache.logging.log4j.core.Filter;
import org.apache.logging.log4j.core.Layout;
import org.apache.logging.log4j.core.LogEvent;
import org.apache.logging.log4j.core.appender.AbstractAppender;
import org.apache.logging.log4j.core.config.plugins.Plugin;
import org.apache.logging.log4j.core.config.plugins.PluginAttribute;
import org.apache.logging.log4j.core.config.plugins.PluginElement;
import org.apache.logging.log4j.core.config.plugins.PluginFactory;
import org.apache.logging.log4j.core.layout.PatternLayout;

import java.io.Serializable;
import java.util.regex.Pattern;


@Plugin(name = "QueueAppender", category = "Core", elementType = "appender", printObject = true)
public class QueueAppender extends AbstractAppender {

    private final IQueueLogger queueLogger;


    protected QueueAppender(String name, Filter filter, Layout<? extends Serializable> layout, boolean ignoreExceptions, IQueueLogger queueLogger) {
        super(name, filter, layout, ignoreExceptions);
        this.queueLogger = queueLogger;
    }

    @PluginFactory
    public static QueueAppender createAppender(@PluginAttribute("name") String name,
                                               @PluginElement("Filter") Filter filter,
                                               @PluginElement("Layout") Layout<? extends Serializable> layout,
                                               @PluginAttribute("loggerClass") String loggerClass,
                                               @PluginAttribute("loggerConfig") String loggerConfig) {

        if(name == null) {
            LOGGER.error("No name provided for QueueAppender");
        }
        System.out.println("Appender");
        if(layout == null) {
            layout = PatternLayout.createDefaultLayout();
        }
        IQueueLogger iQueueLogger = null;
        System.out.println(filter.getClass().getName());
        try{
            Class<?> clazz = Class.forName(loggerClass);
            iQueueLogger = (IQueueLogger) clazz.getConstructor(String.class).newInstance(loggerConfig);
        }catch (Exception e){
            LOGGER.error(e.getMessage());
        }

        return new QueueAppender(name, filter, layout, false, iQueueLogger);

    }

    @Override
    public void append(LogEvent event) {
        byte[] bytes = getLayout().toByteArray(event);
        String message = new String(bytes);
        try {
                queueLogger.send(message);
        }catch (Exception e){
            LOGGER.error(e.getMessage());
        }
    }

    @Override
    public void stop(){
        super.stop();
        if(queueLogger != null){
            queueLogger.close();
        }
    }
}
