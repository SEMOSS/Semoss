import { StrictMode } from "react";
import { createRoot } from "react-dom/client";
import { App } from "./App";
import "./index.css";

const root = document.getElementById("root");
if (!root) {
	throw new Error("Root element #root not found");
}

// Renders <App /> inside of the <div id="root" /> in index.html
createRoot(root).render(
	<StrictMode>
		<App />
	</StrictMode>,
);
