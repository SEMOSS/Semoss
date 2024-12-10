# Python Formatting

This project uses the [Black](https://black.readthedocs.io/en/stable/) code formatter. Black is a Python code formatter that reformats your code to make it more readable. It is a great tool to use to ensure that your code is formatted correctly and consistently. You should ensure that you have Black installed on your machine before making any commits to this project.

## Instructions
The following instructions assume that you are using Visual Studio Code as your code editor. If you are using a different code editor, you will need to adjust the instructions accordingly.

1. *Install Black Formatter Extension in VSCode*
   - Click on the Extensions icon in the Activity Bar on the side of the window
   - Search for "Black Formatter" in the Extensions view search box
   - Click on the Install button next to the Black Formatter extension provided by Microsoft

2. *Create a .vscode directory*
    - In the root of the py folder, create a new directory called .vscode
    - Inside the .vscode directory, create a new file called settings.json
    - Add the following code to the settings.json file inside of the main brackets:
    ```json
    "[python]": {
        "editor.formatOnType": true,
        "editor.defaultFormatter": "ms-python.black-formatter",
        "editor.formatOnSave": true
      },
    ```
    - Save the settings.json file

3. *Update your User Settings (optional)*
    - If you plan to work on other Python related CFG projects such as the Python SDK, you may want to update your User Settings to enable the Black formatter for all Python projects
    - Open the Command Palette by pressing `Ctrl+Shift+P`
    - Type "Preferences: Open User Settings (JSON)" and select it from the dropdown
    - Add the following code to the settings.json file:
    ```json
    "[python]": {
        "editor.formatOnType": true,
        "editor.defaultFormatter": "ms-python.black-formatter",
        "editor.formatOnSave": true
      },
    ```
    - Save the settings.json file