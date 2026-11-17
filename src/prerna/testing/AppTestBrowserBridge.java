package prerna.testing;

/** Platform service worker: scopes routing to individual browser clients, before app JavaScript executes. */
public final class AppTestBrowserBridge {
	private AppTestBrowserBridge() { }
	public static String script() {
		return """
"use strict";
const PARAM = "__semoss_app_test";
const EXIT = "__semoss_app_test_exit";
const HEADER = "X-Semoss-App-Test";
const moduleUrl = new URL(self.registration.scope);
const databaseName = "semoss-app-test-contexts:" + moduleUrl.pathname;
function openDatabase() {
  return new Promise((resolve, reject) => {
    const request = indexedDB.open(databaseName, 1);
    request.onupgradeneeded = () => request.result.createObjectStore("clients");
    request.onsuccess = () => resolve(request.result);
    request.onerror = () => reject(new Error("Test context storage is unavailable"));
  });
}
async function readClient(id) {
  if (!id) return null;
  const db = await openDatabase();
  try {
    return await new Promise((resolve, reject) => {
      const request = db.transaction("clients").objectStore("clients").get(id);
      request.onsuccess = () => resolve(request.result || null);
      request.onerror = () => reject(request.error);
    });
  } finally { db.close(); }
}
async function writeClient(id, context) {
  if (!id) throw new Error("This browser did not provide a test navigation client id");
  const db = await openDatabase();
  try {
    await new Promise((resolve, reject) => {
      const transaction = db.transaction("clients", "readwrite");
      transaction.objectStore("clients").put(context, id);
      transaction.oncomplete = resolve;
      transaction.onerror = () => reject(transaction.error);
      transaction.onabort = () => reject(transaction.error);
    });
  } finally { db.close(); }
}
function explicitContext(url) {
  const token = url.searchParams.get(PARAM);
  if (!token) return null;
  if (!/^[a-f0-9-]{36}$/.test(token)) throw new Error("Invalid app test context");
  const base = moduleUrl.pathname + "public_home/";
  if (!url.pathname.startsWith(base)) throw new Error("Test navigation is outside the app portal");
  const suffix = url.pathname.slice(base.length);
  const split = suffix.indexOf("/portals/");
  if (split < 1 || suffix.slice(0, split).includes("/")) throw new Error("Invalid test app portal");
  return { token, portal: base + suffix.slice(0, split) + "/portals/" };
}
async function navigationContext(event) {
  const referrer = event.request.referrer;
  if (referrer) {
    const explicit = explicitContext(new URL(referrer));
    if (explicit) return explicit;
  }
  // Navigation clientId is not available in every browser. Inspect the referring live client.
  const clients = await self.clients.matchAll({ type: "window", includeUncontrolled: true });
  const matches = clients.filter(client => referrer && client.url === referrer);
  const values = await Promise.all(matches.map(client => readClient(client.id)));
  const tests = values.filter(value => value && value.token);
  if (tests.length) {
    if (tests.length !== values.length || tests.some(value => value.token !== tests[0].token))
      throw new Error("Ambiguous test navigation; reopen the preview from Testing");
    return tests[0];
  }
  return null;
}
self.addEventListener("install", event => event.waitUntil(self.skipWaiting()));
self.addEventListener("activate", event => event.waitUntil(self.clients.claim()));
self.addEventListener("message", event => {
  if (event.data && event.data.type === "SEMOSS_APP_TEST_READY") {
    event.waitUntil(openDatabase().then(db => { db.close(); event.ports[0]?.postMessage({ ready: "replacesClientId" in FetchEvent.prototype }); })
      .catch(() => event.ports[0]?.postMessage({ ready: false })));
  }
  if (event.data && event.data.type === "SEMOSS_APP_TEST_STATUS") {
    event.waitUntil(self.clients.matchAll({ type: "window", includeUncontrolled: true })
      .then(clients => Promise.all(clients.map(client => readClient(client.id))))
      .then(values => event.ports[0]?.postMessage({ active: values.some(value => value && value.token === event.data.token && value.verified) }))
      .catch(() => event.ports[0]?.postMessage({ active: false })));
  }
});
self.addEventListener("fetch", event => {
  event.respondWith((async () => {
    const url = new URL(event.request.url);
    const navigate = event.request.mode === "navigate";
    let context = navigate ? explicitContext(url) : null;
    if (!context) context = await readClient(event.clientId || (navigate ? event.replacesClientId : ""));
    if (!context && navigate) context = await navigationContext(event);
    if (context && context.token && navigate && url.searchParams.get(EXIT) === context.token) {
      if (url.origin !== moduleUrl.origin || !url.pathname.startsWith(context.portal)) throw new Error("Invalid test exit");
      // The old client stays marked: requests already in flight cannot become normal requests.
      await writeClient(event.resultingClientId, { normal: true });
      const clean = new URL(url); clean.searchParams.delete(EXIT); clean.searchParams.delete(PARAM);
      const headers = new Headers(event.request.headers); headers.delete(HEADER);
      return fetch(new Request(clean, { method: "GET", headers, credentials: "same-origin", redirect: "error" }));
    }
    if (!context || !context.token) return fetch(event.request);
    if (url.origin !== moduleUrl.origin || !url.pathname.startsWith(moduleUrl.pathname))
      throw new Error("External requests are blocked in test database mode");
    if (navigate && !url.pathname.startsWith(context.portal)) throw new Error("Test navigation left the target app");
    if (navigate) await writeClient(event.resultingClientId, context);
    const headers = new Headers(event.request.headers); headers.set(HEADER, context.token);
    if (navigate) {
      const proof = await fetch(new URL("app-test-context", moduleUrl), { headers: { [HEADER]: context.token }, credentials: "same-origin", redirect: "error", cache: "no-store" });
      if (!proof.ok || proof.headers.get("X-Semoss-App-Test-Active") !== context.token)
        throw new Error("Backend test routing is not installed");
    }
    const response = await fetch(new Request(event.request, { headers, credentials: "same-origin", redirect: "error" }));
    if (response.headers.get("X-Semoss-App-Test-Active") !== context.token)
      throw new Error("The backend did not confirm test routing");
    if (navigate) await writeClient(event.resultingClientId, { ...context, verified: response.ok });
    return response;
  })().catch(() => new Response("App test routing failed. No production fallback was attempted. Reopen the preview from Testing.", {
    status: 409, headers: { "Content-Type": "text/plain; charset=utf-8", "Cache-Control": "no-store" }
  })));
});
""";
	}
}
