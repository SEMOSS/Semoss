package prerna.reactor.playwright;

public record ElementMetrics(
        int offsetWidth,  int offsetHeight,
        int clientWidth,  int clientHeight,
        int scrollWidth,  int scrollHeight
) {}