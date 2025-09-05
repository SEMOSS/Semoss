## Purpose

The `java/` folder contains the custom reactors consumed by the application from `client/`.

## Install

Following the core SEMOSS install instructions, we will focus on using Eclipse as the IDE of choice.

### Project Import

Install the project with `File > Import > Existing Maven Projects > Browse`. Navigate to the the SEMOSS project directory where you installed these files (e.g., `C:\workspace\Semoss\project\SemossBlankCanvas__[APP_ID]\app_root\version\assets`), and click `Select Folder`. Click `Finish`.

This should import the reactor files into your workspace. You may not see the project appear in your `Project Explorer` panel by default since it is nested in the `Semoss` project from the SEMOSS install instructions. You can click the vertical triple dots button to the top right of your project explorer and check that `Projects Presentation` shows as `Flat` instead of `Hierarchical` to get it to appear non-nested.

### Code Style

The Blank Canvas repository enforces the Google Java Style Guide as a pre-commit hook to enforce code style. Code changes will be performed automatically at commit time to align with the guide, but you may want to update Eclipse's Java formatter to make these changes before commit time. You can install Google's formatter plugin by following the instructions [here](https://github.com/google/google-java-format?tab=readme-ov-file#eclipse). In short: download the (plugin)[https://github.com/google/google-java-format/releases/download/v1.23.0/google-java-format-eclipse-plugin-1.23.0.jar], and copy to `C:\Users\<youruser>\eclipse\dropins` (or wherever you installed Eclipse). Once there you can right click your TQMC project, choose `Properties > Java Code Style > Formatter`, click the checkbox to `Enable project specific settings` and choose `google-java-format` in the dropdown for `Formatter implementation:`. After applying, you should be able to format a given source file to the new style guide with `Ctrl+Shift+F` or via `Source > Format` as per usual.
