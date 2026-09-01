/**
 * Renders the home page, currently displaying an example component.
 *
 * @component
 */
export const HomePage = () => {
  return (
    <div className="flex flex-col items-center justify-center h-full gap-3">
      <h1 className="text-5xl font-light tracking-widest text-foreground">
        SEMOSS
      </h1>
      <p className="text-sm text-muted-foreground tracking-wide">
        Template App
      </p>
    </div>
  );
};
