# React client

`client/` is the editable React/Vite source. `../portals/` is generated output
served by SEMOSS and should be regenerated rather than edited by hand.

When this project is created from the platform template, SEMOSS writes the new
project UUID to `client/.env` as `APP`. Preserve that value: the SDK uses it to
run Pixels in the cloned app's context.

App Builder agents must use `BuildAndPublishApp(project=<current-project-id>)`
after changing client source. It invokes the supported builder, replaces
`portals/`, publishes the app, and releases the updated project. Do not use
shell Node, npm, or pnpm commands from an App Builder run.
