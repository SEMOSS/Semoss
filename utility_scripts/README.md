# Utility scripts

Developer tools for repository maintenance and asset generation. Keep runtime
Python modules in `py/` and development-only scripts here.

This directory is outside the folders included by [`semosshome.xml`](../semosshome.xml),
so these scripts are not bundled in the `semosshome` package used by the Docker
images.

## Generate catalog stock images

Requires Python with Pillow and NumPy installed. From the repository root:

```sh
python utility_scripts/generate_stock_engine_images.py
```

The script creates paired light/dark PNGs in `images/stock-engines-light/` and
`images/stock-engines-dark/`, plus a preview in
`images/previews/stock-engines/preview.png`. It resolves these paths relative to
the repository, so it can also be run from another working directory.

This is the only preview the script generates. The retained system-project and
system-app preview sheets are maintained separately from their SVG sources;
the script leaves them untouched and does not create close-ups or catalog screenshots.

Use `--output-root DIR` to render elsewhere or `--preview-dir DIR` to override
the preview location. See [`images/previews`](../images/previews/README.md) for
the current artwork.
