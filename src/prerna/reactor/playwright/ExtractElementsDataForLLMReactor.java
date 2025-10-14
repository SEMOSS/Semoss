package prerna.reactor.playwright;

import java.util.HashMap;
import java.util.Map;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.microsoft.playwright.Page;

import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.Utility;

//returns elements data in the selected area to be used by LLM for generating playwright steps
public class ExtractElementsDataForLLMReactor extends AbstractReactor {
    
    ObjectMapper json = new ObjectMapper().enable(SerializationFeature.INDENT_OUTPUT);
    
    public ExtractElementsDataForLLMReactor() {
        this.keysToGet = new String[] {
            "sessionId",
            ReactorKeysEnum.PARAM_VALUES_MAP.getKey()
        };
        this.keyRequired = new int[] { 1, 1 };
    }


    //getReactorDescription
    @Override
    public String getReactorDescription() {
        return "Extracts interactive HTML elements data from a specified area of the webpage for LLM processing.";
    }

    @Override
    public NounMetadata execute() {
        organizeKeys();
        String sessionId = this.keyValue.get(this.keysToGet[0]);
        Map<String, Object> paramValues = Utility.getMap(this.store, this.curRow);
        
        Map<String, Object> result = extractHtml(sessionId, paramValues);
        return new NounMetadata(result, PixelDataType.MAP);
    }
    
    private Map<String, Object> extractHtml(String sessionId, Map<String, Object> params) {
        Session s = SessionReactor.get(sessionId);
        Page page = s.page;
        
        // Get coordinates from params
        int startX = ((Number) params.get("startX")).intValue();
        int startY = ((Number) params.get("startY")).intValue();
        int endX = ((Number) params.get("endX")).intValue();
        int endY = ((Number) params.get("endY")).intValue();
        
        // Execute JavaScript to extract HTML from the selected area
        @SuppressWarnings("unchecked")
        Map<String, Object> result = (Map<String, Object>) page.evaluate(
            JS_EXTRACT_HTML, 
            new Object[] { startX, startY, endX, endY }
        );
        
        if (result == null) {
            // Return empty result if nothing found
            Map<String, Object> emptyResponse = new HashMap<>();
            emptyResponse.put("html", "");
            emptyResponse.put("elements", new java.util.ArrayList<>());
            emptyResponse.put("elementCount", 0);
            emptyResponse.put("interactiveCount", 0);
            
            Map<String, Object> bounds = new HashMap<>();
            bounds.put("startX", startX);
            bounds.put("startY", startY);
            bounds.put("endX", endX);
            bounds.put("endY", endY);
            emptyResponse.put("bounds", bounds);
            
            return emptyResponse;
        }
        
        return result;
    }
    
    private static final String JS_EXTRACT_HTML = """
    ([startX, startY, endX, endY]) => {
        function getCssPath(el) {
            if (!el) return "";
            if (el.id) return "#" + el.id;
            
            let path = [];
            let current = el;
            
            while (current && current !== document.body) {
                let selector = current.tagName.toLowerCase();
                if (current.id) {
                    path.unshift("#" + current.id);
                    break;
                } else if (current.className) {
                    const classes = current.className.split(' ')
                        .filter(c => c && !c.startsWith('ext-'))
                        .slice(0, 2);
                    if (classes.length > 0) {
                        selector += '.' + classes.join('.');
                    }
                }
                path.unshift(selector);
                current = current.parentElement;
                if (path.length > 4) break; // Keep paths short
            }
            return path.join(' > ');
        }
        
        function isInteractive(el) {
            const tag = el.tagName.toLowerCase();
            
            // Primary interactive elements
            if (['input', 'button', 'select', 'textarea', 'a'].includes(tag)) {
                return true;
            }
            
            // Check role
            const role = el.getAttribute('role');
            if (role && ['button', 'link', 'textbox', 'checkbox', 'radio'].includes(role)) {
                return true;
            }
            
            // Check for click handlers
            if (el.onclick || el.getAttribute('onclick')) return true;
            
            // Check class names for buttons
            const className = (el.className.baseVal || el.className || '').toString().toLowerCase();
            if (className.includes('btn') || className.includes('button')) {
                return true;
            }
            
            // Check cursor style
            const cs = getComputedStyle(el);
            if (cs.cursor === 'pointer') return true;
            
            return false;
        }
        
        function getElementPurpose(el, attrs) {
            const tag = el.tagName.toLowerCase();
            const type = attrs.type || '';
            const name = (attrs.name || '').toLowerCase();
            const placeholder = (attrs.placeholder || '').toLowerCase();
            const ariaLabel = (attrs['aria-label'] || '').toLowerCase();
            
            // Detect input types
            if (tag === 'input') {
                if (type === 'password') return 'password-field';
                if (type === 'email' || name.includes('email') || placeholder.includes('email')) {
                    return 'email-field';
                }
                if (name.includes('user') || placeholder.includes('user') || 
                    ariaLabel.includes('user')) {
                    return 'username-field';
                }
                if (type === 'submit') return 'submit-button';
                
                return 'input-' + ariaLabel + '-field';
                return 'input-field';
            }
            
            if (tag === 'button' || attrs.role === 'button') {
                const text = el.innerText.toLowerCase();
                if (text.includes('sign in') || text.includes('login') || 
                    text.includes('submit') || type === 'submit') {
                    return 'submit-button';
                }
                return 'button';
            }
            
            if (tag === 'a') return 'link';
            
            return 'interactive';
        }
        
        function isElementVisible(el, rect) {
            if (rect.width === 0 || rect.height === 0) return false;
            
            const cs = getComputedStyle(el);
            if (cs.display === 'none' || cs.visibility === 'hidden' || 
                cs.opacity === '0') return false;
            
            // Check if element is off-screen (like hidden password fields)
            if (rect.x < -1000 || rect.y < -1000) return false;
            
            return true;
        }
        
        function isInBounds(rect) {
            const centerX = rect.left + rect.width / 2;
            const centerY = rect.top + rect.height / 2;
            return centerX >= startX && centerX <= endX && 
                   centerY >= startY && centerY <= endY;
        }

        function getNearbyLabels(el, rect) {
            const labels = [];
            const radius = 100; // pixels
            
            // Check all text nodes near the element
            const walker = document.createTreeWalker(
                document.body,
                NodeFilter.SHOW_TEXT,
                null
            );
            
            let node;
            while (node = walker.nextNode()) {
                const parent = node.parentElement;
                if (!parent || !node.nodeValue.trim()) continue;
                
                const parentRect = parent.getBoundingClientRect();
                const distance = Math.sqrt(
                    Math.pow(parentRect.x - rect.x, 2) + 
                    Math.pow(parentRect.y - rect.y, 2)
                );
                
                if (distance < radius && node.nodeValue.trim().length > 2) {
                    labels.push({
                        text: node.nodeValue.trim(),
                        distance: Math.round(distance),
                        position: parentRect.y < rect.y ? 'above' : 
                                 parentRect.y > rect.y + rect.height ? 'below' :
                                 parentRect.x < rect.x ? 'left' : 'right'
                    });
                }
            }
            
            return labels.slice(0, 3); // Top 3 closest
        }

        function getParentContext(el) {
            const contexts = [];
            let current = el.parentElement;
            let depth = 0;
            
            while (current && depth < 5) {
                // Get meaningful class names
                const classes = (current.className.baseVal || current.className || '')
                    .toString()
                    .split(' ')
                    .filter(c => c && 
                            !c.startsWith('ng-') && 
                            !c.startsWith('ui-') &&
                            c.length > 2)
                    .slice(0, 2);
                
                if (classes.length > 0) {
                    contexts.push({
                        tag: current.tagName.toLowerCase(),
                        classes: classes
                    });
                }
                
                // Check for ID
                if (current.id && !current.id.startsWith('ext-')) {
                    contexts.push({
                        tag: current.tagName.toLowerCase(),
                        id: current.id
                    });
                }
                
                current = current.parentElement;
                depth++;
            }
            
            return contexts.slice(0, 2); // Top 2 most relevant parents
        }

        function getSectionHeader(el) {
            let current = el;
            
            // Walk up and back in DOM to find nearest heading
            while (current) {
                const prev = current.previousElementSibling;
                if (prev) {
                    const heading = prev.querySelector('h1, h2, h3, h4, h5, h6');
                    if (heading) return heading.innerText.trim();
                    
                    if (/^h[1-6]$/i.test(prev.tagName)) {
                        return prev.innerText.trim();
                    }
                }
                current = current.parentElement;
            }
            
            return null;
        }

        function getTableContext(el) {
            const cell = el.closest('td, th');
            if (!cell) return null;
            
            const row = cell.closest('tr');
            const table = cell.closest('table');
            
            if (!table) return null;
            
            // Get column header
            const cellIndex = Array.from(row.children).indexOf(cell);
            const thead = table.querySelector('thead');
            let columnHeader = null;
            
            if (thead) {
                const headerRow = thead.querySelector('tr');
                if (headerRow && headerRow.children[cellIndex]) {
                    columnHeader = headerRow.children[cellIndex].innerText.trim();
                }
            }
            
            return {
                columnHeader: columnHeader,
                rowIndex: Array.from(table.querySelectorAll('tr')).indexOf(row),
                columnIndex: cellIndex
            };
        }
        
        // Find only interactive elements in bounds
        const allElements = document.querySelectorAll('*');
        let interactive = [];
        
        for (const el of allElements) {
            const rect = el.getBoundingClientRect();
            
            if (!isInBounds(rect)) continue;
            if (!isInteractive(el)) continue;
            if (!isElementVisible(el, rect)) continue;
            
            const tag = el.tagName.toLowerCase();
            
            // Get key attributes
            const attrs = {};
            ['id', 'name', 'class', 'type', 'placeholder', 'value', 
             'aria-label', 'role', 'href'].forEach(attr => {
                const val = el.getAttribute(attr);
                if (val) attrs[attr] = val;
            });
            
            const nearbyLabels = getNearbyLabels(el, rect);
            const parentContext = getParentContext(el);
            const sectionHeader = getSectionHeader(el);
            const tableContext = getTableContext(el);

            const purpose = getElementPurpose(el, attrs);
            const text = (el.innerText || el.textContent || '').trim().slice(0, 100);
            
            interactive.push({
                tag: tag,
                purpose: purpose,
                text: text,
                selector: getCssPath(el),
                coords: {
                    x: Math.round(rect.x + rect.width / 2),
                    y: Math.round(rect.y + rect.height / 2)
                },
                rect: {
                    x: Math.round(rect.x),
                    y: Math.round(rect.y),
                    width: Math.round(rect.width),
                    height: Math.round(rect.height)
                },
                attributes: attrs,
                nearbyLabels: nearbyLabels, 
                parentContext: parentContext, 
                sectionHeader: sectionHeader, 
                tableContext: tableContext,  
                interactive: true,
                visible: true
            });
        }
        
        const deduplicated = [];

        // Sort by DOM depth (shallowest first) - your approach is good
        interactive.sort((a, b) => {
            const depthA = a.selector.split('>').length;
            const depthB = b.selector.split('>').length;
            return depthA - depthB;
        });

        for (const el of interactive) {
            // Check if this element is a child of an already-added element
            const isDuplicate = deduplicated.some(parent => {
                // Avoid repeated querySelector calls - check geometry first
                const rectsOverlap = 
                    Math.abs(parent.rect.x - el.rect.x) < 10 &&
                    Math.abs(parent.rect.y - el.rect.y) < 10 &&
                    Math.abs(parent.rect.width - el.rect.width) < 10 &&
                    Math.abs(parent.rect.height - el.rect.height) < 10;
                
                if (!rectsOverlap) return false;
                
                // Only if geometry matches, check DOM containment
                // Better: use the actual element reference if you stored it
                const parentEl = document.querySelector(parent.selector);
                const currentEl = document.querySelector(el.selector);
                
                return parentEl && currentEl && parentEl.contains(currentEl);
            });
            
            if (!isDuplicate) {
                deduplicated.push(el);
            }
        }

        interactive = deduplicated;

        // Sort by position (top to bottom, left to right)
        interactive.sort((a, b) => {
            const dy = a.rect.y - b.rect.y;
            if (Math.abs(dy) > 20) return dy;
            return a.rect.x - b.rect.x;
        });



        //limit to 30 elements
        if (interactive.length > 20) {
            interactive.splice(20);
        }
        
        // Detect form structure
        const hasEmail = interactive.some(e => e.purpose === 'email-field');
        const hasUsername = interactive.some(e => e.purpose === 'username-field');
        const hasPassword = interactive.some(e => e.purpose === 'password-field');
        const hasSubmit = interactive.some(e => e.purpose === 'submit-button');
        const isLoginForm = (hasEmail || hasUsername) && (hasPassword || hasSubmit);
        
        return {
            elements: interactive,
            elementCount: interactive.length,
            interactiveCount: interactive.length,
            bounds: {
                startX: startX,
                startY: startY,
                endX: endX,
                endY: endY,
                width: endX - startX,
                height: endY - startY
            },
            summary: {
                totalElements: interactive.length,
                interactiveElements: interactive.length,
                isLoginForm: isLoginForm,
                hasEmail: hasEmail,
                hasUsername: hasUsername,
                hasPassword: hasPassword,
                hasSubmit: hasSubmit
            }
        };
    }
    """;
}