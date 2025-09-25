package prerna.reactor.playwright;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.sablecc2.om.PixelDataType;
import com.microsoft.playwright.Page;


import java.util.Map;

public class ProbeElementReactor extends AbstractReactor {

    ObjectMapper json = new ObjectMapper().enable(SerializationFeature.INDENT_OUTPUT);

    public ProbeElementReactor() {
        this.keysToGet = new String[]{
                "sessionId",
                "coords" 
        };
        this.keyRequired = new int[]{1, 1}; // both required
    }

    @Override
    public NounMetadata execute() {
        organizeKeys();

        String sessionId = this.keyValue.get(this.keysToGet[0]);
        String coordsStr = this.keyValue.get(this.keysToGet[1]); 

        Coords coords = parseCoords(coordsStr);

        ElementProbeResponse response = probeElementAt(sessionId, coords);

        // test if the return format can be processed by the backend
        Map<String, Object> asMap = json.convertValue(response, Map.class);
        return new NounMetadata(asMap, PixelDataType.MAP);
    }

    private Coords parseCoords(String coordsStr) {
        // Simple comma-separated "x,y"
        String[] parts = coordsStr.split(",");
        if (parts.length != 2) {
            throw new IllegalArgumentException("Invalid coords format. Expected 'x,y' but got: " + coordsStr);
        }
        return new Coords(Integer.parseInt(parts[0].trim()), Integer.parseInt(parts[1].trim()));
    }

    public static ElementProbeResponse probeElementAt(String sessionId, Coords coords) {
        Session s = SessionReactor.get(sessionId);
        Page page = s.page;

        int x = coords.x();
        int y = coords.y();

        Map<String, Object> data = (Map<String, Object>) page.evaluate(JS_PROBE, new Object[]{x, y});

        if (data == null) {
            return new ElementProbeResponse(
                    null, null, null, null, null, null, null, null, false,
                    new ElementRect(0, 0, 0, 0),
                    new ElementMetrics(0, 0, 0, 0, 0, 0),
                    null, null, null, false
            );
        }

        Map<String, Object> rect = (Map<String, Object>) data.get("rect");
        ElementRect er = new ElementRect(
                ((Number) rect.get("x")).doubleValue(),
                ((Number) rect.get("y")).doubleValue(),
                ((Number) rect.get("width")).doubleValue(),
                ((Number) rect.get("height")).doubleValue()
        );

        Map<String, Object> m = (Map<String, Object>) data.get("metrics");
        ElementMetrics metrics = new ElementMetrics(
                ((Number) m.getOrDefault("offsetWidth", 0)).intValue(),
                ((Number) m.getOrDefault("offsetHeight", 0)).intValue(),
                ((Number) m.getOrDefault("clientWidth", 0)).intValue(),
                ((Number) m.getOrDefault("clientHeight", 0)).intValue(),
                ((Number) m.getOrDefault("scrollWidth", 0)).intValue(),
                ((Number) m.getOrDefault("scrollHeight", 0)).intValue()
        );

        Map<String, String> styles = (Map<String, String>) data.get("styles");
        Map<String, String> placeholderStyle = (Map<String, String>) data.get("placeholderStyle");
        Map<String, String> attrs = (Map<String, String>) data.get("attrs");
        boolean isTextControl = (Boolean) data.getOrDefault("isTextControl", false);

        return new ElementProbeResponse(
                (String) data.get("tag"),
                (String) data.get("type"),
                (String) data.get("role"),
                (String) data.get("selector"),
                (String) data.get("placeholder"),
                (String) data.get("labelText"),
                (String) data.get("value"),
                (String) data.get("href"),
                (Boolean) data.get("contentEditable"),
                er,
                metrics,
                styles,
                placeholderStyle,
                attrs,
                isTextControl
        );
    }

    private static final String JS_PROBE = """
        ([x,y]) => {
          const el = document.elementFromPoint(x,y);
          if (!el) return null;

          const r  = el.getBoundingClientRect();
          const cs = getComputedStyle(el);

          const pick = (src, names) => {
            const out = {};
            for (const n of names) out[n] = src[n];
            return out;
          };

          const styleProps = [
            "boxSizing","display","visibility","opacity",
            "width","height","minWidth","minHeight","maxWidth","maxHeight",
            "marginTop","marginRight","marginBottom","marginLeft",
            "paddingTop","paddingRight","paddingBottom","paddingLeft",
            "borderTopWidth","borderRightWidth","borderBottomWidth","borderLeftWidth",
            "borderTopStyle","borderRightStyle","borderBottomStyle","borderLeftStyle",
            "borderTopColor","borderRightColor","borderBottomColor","borderLeftColor",
            "borderTopLeftRadius","borderTopRightRadius","borderBottomRightRadius","borderBottomLeftRadius",
            "outlineWidth","outlineStyle","outlineColor","outlineOffset","boxShadow","textShadow",
            "color","backgroundColor","backgroundImage","backgroundClip",
            "fontFamily","fontSize","fontWeight","fontStyle","fontStretch","fontVariant",
            "lineHeight","letterSpacing","textAlign","textTransform",
            "textDecorationLine","textDecorationStyle","textDecorationColor",
            "whiteSpace","wordBreak","direction","writingMode",
            "caretColor","overflow","overflowX","overflowY"
          ];
          const styles = pick(cs, styleProps);

          let placeholderStyle = null;
          try {
            const ph = getComputedStyle(el, "::placeholder");
            if (ph) {
              placeholderStyle = pick(ph, [
                "color","opacity","fontStyle","fontWeight","fontSize","fontFamily","letterSpacing"
              ]);
            }
          } catch (e) {}

          const metrics = {
            offsetWidth:  el.offsetWidth,
            offsetHeight: el.offsetHeight,
            clientWidth:  el.clientWidth,
            clientHeight: el.clientHeight,
            scrollWidth:  el.scrollWidth,
            scrollHeight: el.scrollHeight
          };

          function cssPath(e){
            if (!e) return "";
            if (e.id) return e.tagName.toLowerCase() + "#" + e.id;
            let sel = e.tagName.toLowerCase();
            const p = e.parentElement;
            if (!p) return sel;
            const idx = Array.from(p.children).indexOf(e) + 1;
            sel += ":nth-of-type(" + idx + ")";
            return cssPath(p) + ">" + sel;
          }

          let labelText = "";
          if (el.labels && el.labels.length) labelText = el.labels[0].innerText.trim();
          if (!labelText) labelText = el.getAttribute("aria-label") || "";
          if (!labelText) {
            const lab = el.closest("label");
            if (lab) labelText = lab.innerText.trim();
          }

          const attrNames = [
            "id","name","class","placeholder",
            "autocomplete","inputmode","pattern","maxlength","minlength","size",
            "dir","lang","list","step","min","max","form","wrap","cols","rows",
            "aria-label","aria-labelledby","aria-describedby"
          ];
          const attrs = {};
          for (const n of attrNames) {
            const v = el.getAttribute(n);
            if (v !== null) attrs[n] = v;
          }

          const tag = el.tagName.toLowerCase();
          const isTextControl = tag === "input" || tag === "textarea";

          return {
            tag,
            type: (el.type || "") + "",
            role: el.getAttribute("role") || "",
            selector: cssPath(el),
            placeholder: el.getAttribute("placeholder") || "",
            labelText,
            value: (("value" in el) ? (el.value || "") : ""),
            href: el.getAttribute("href") || "",
            contentEditable: el.isContentEditable === true,
            rect: { x: r.x, y: r.y, width: r.width, height: r.height },
            metrics, styles, placeholderStyle, attrs, isTextControl
          };
        }
        """;
}
