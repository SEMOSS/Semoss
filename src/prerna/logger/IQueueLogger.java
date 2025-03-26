package prerna.logger;

import java.util.Properties;

public interface IQueueLogger {
    void send(String message);
    void close();

    public default Properties getProperties(String config){
        Properties prop = new Properties();
        for(String entry: config.split(",")){
            String[] keyValue = entry.split("=");
            prop.put(keyValue[0].trim(), keyValue[1].trim());
        }
        return prop;
    }
}
