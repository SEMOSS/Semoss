import { Env, runPixel } from "@semoss/sdk";
import { useState } from "react";

const sourceEngineId = "REPLACE_WITH_SYNTHETIC_SOURCE_ENGINE_ID";
const portalIndex = window.location.pathname.indexOf("/public_home/");
const modulePath =
	portalIndex < 0 ? "" : window.location.pathname.slice(0, portalIndex);
Env.update({ MODULE: window.location.origin + modulePath });

interface TaskResult {
	data: { values: [string][] };
}

/** Exercises the real SDK read and write path with a single synthetic task. */
export const App = () => {
	const [title, setTitle] = useState("");
	const [error, setError] = useState("");
	const [isWorking, setIsWorking] = useState(false);

	const loadTitle = async (): Promise<void> => {
		const sql = "SELECT TITLE FROM TASKS WHERE ID=1";
		const result = await runPixel<[TaskResult]>(
			"SqlQuery(database=[" + JSON.stringify(sourceEngineId) +
			"], query=[" + JSON.stringify(sql) + "]);",
		);
		const title = result.pixelReturn[0]?.output.data.values[0]?.[0];
		if (result.errors.length || typeof title !== "string") {
			throw new Error("Could not load the task");
		}
		setTitle(title);
	};

	const handleTask = async (markDone: boolean): Promise<void> => {
		setIsWorking(true);
		setError("");
		try {
			if (markDone) {
				const sql = "UPDATE TASKS SET TITLE='Done' WHERE ID=1";
				const result = await runPixel<[boolean]>(
					"SqlQuery(database=[" + JSON.stringify(sourceEngineId) +
					"], query=[" + JSON.stringify(sql) + "]);",
				);
				if (result.errors.length || result.pixelReturn[0]?.output !== true) {
					throw new Error("Could not update the task");
				}
			}
			await loadTitle();
		} catch (failure) {
			setError(failure instanceof Error ? failure.message : "The task failed");
		} finally {
			setIsWorking(false);
		}
	};

	return (
		<main>
			<h1>Tasks</h1>
			<button type="button" data-testid="load-task" onClick={() => void handleTask(false)} disabled={isWorking}>
				Load Task
			</button>
			<button type="button" data-testid="complete-task" onClick={() => void handleTask(true)} disabled={isWorking || !title}>
				Mark Done
			</button>
			<p data-testid="task-title" role="status">{title}</p>
			{error && <p role="alert">{error}</p>}
		</main>
	);
};
