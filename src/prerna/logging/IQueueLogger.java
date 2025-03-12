package prerna.logging;

public interface IQueueLogger {
    void send(String message);
    void close();
}
