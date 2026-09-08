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
      // Use the server-provided theme name or default below when the map is invalid.
    }
  }

  return typeof theme?.THEME_NAME === "string" && theme.THEME_NAME.trim()
    ? theme.THEME_NAME.trim()
    : "SEMOSS";
};

/**
 * Renders the home page, currently displaying an example component.
 *
 * @component
 */
export const HomePage = () => {
  const { system } = useInsight();
  const themeName = getThemeName(system?.config?.theme);

  useEffect(() => {
    document.title = themeName;
  }, [themeName]);

  return (
    <div className="flex flex-col items-center justify-center h-full gap-3">
      <h1 className="text-5xl font-light tracking-widest text-foreground">
        {themeName}
      </h1>
      <p className="text-sm text-muted-foreground tracking-wide">
        Template App
      </p>
    </div>
  );
};
