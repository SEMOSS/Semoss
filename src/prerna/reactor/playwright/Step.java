package prerna.reactor.playwright;

public record Step(
        int id,
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
        String label,
        boolean isPassword,
        boolean storeValue,
        Selector selector,
        TriggerNewTab isTriggerNewTab
) {

    Step(Step s, String text) {
        this(s.id, s.type, s.url, s.coords, text, s.pressEnter, s.deltaY, s.waitUntil, s.waitAfterMs,
                s.viewport, s.timestamp, s.label, s.isPassword, s.storeValue, s.selector, s.isTriggerNewTab);
    }

    Step(Step s, int id) {
        this(id, s.type, s.url, s.coords, s.text, s.pressEnter, s.deltaY, s.waitUntil, s.waitAfterMs,
                s.viewport, s.timestamp, s.label, s.isPassword, s.storeValue, s.selector, s.isTriggerNewTab);
    }

    Step(Step s, String label, String text, boolean storeValue) {
        this(s.id, s.type, s.url, s.coords, text, s.pressEnter, s.deltaY, s.waitUntil, s.waitAfterMs,
                s.viewport, s.timestamp, label, s.isPassword, storeValue, s.selector, s.isTriggerNewTab);
    }
}
