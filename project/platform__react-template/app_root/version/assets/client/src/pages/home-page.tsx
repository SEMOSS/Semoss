import { useInsight } from "@semoss/sdk/react";
import { useEffect } from "react";

const getThemeName = (theme: Record<string, unknown> | undefined): string => {
	const themeMap = theme?.THEME_MAP;

	if (typeof themeMap === "string") {
		try {
			const name = (JSON.parse(themeMap) as { name?: unknown }).name;
			if (typeof name === "string" && name.trim()) {
				return name.trim();
			}
		} catch {
			// Fall back to the server-provided theme name or the default below
		}
	}

	return typeof theme?.THEME_NAME === "string" && theme.THEME_NAME.trim()
		? theme.THEME_NAME.trim()
		: "SEMOSS";
};

/**
 * Placeholder home page. Replace it with the app's own content.
 */
export const HomePage = () => {
	const { system } = useInsight();
	const themeName = getThemeName(system?.config?.theme);

	useEffect(() => {
		document.title = themeName;
	}, [themeName]);

	return (
		<div className="flex h-full flex-col items-center justify-center gap-3">
			<h1 className="text-5xl font-light tracking-widest text-foreground">
				{themeName}
			</h1>
			<p className="text-sm tracking-wide text-muted-foreground">
				Template App
			</p>
		</div>
	);
};
