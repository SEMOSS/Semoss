package prerna.reactor.playwright;

public record Step(
        StepType type,
        String url,                 // for NAVIGATE
        Coords coords,              // for CLICK/TYPE/SCROLL
        String text,                // for TYPE
        Boolean pressEnter,         // for TYPE
        Integer deltaY,             // for SCROLL
        String waitUntil,           // for NAVIGATE
        Integer waitAfterMs,        // generic wait after action
        Viewport viewport,          // viewport the coords were computed against
        Long timestamp,
        String label
) {}