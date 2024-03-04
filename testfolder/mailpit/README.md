# Hello EMAIL API Testers

Please paste the mailpit.exe file in this directory. You can get it at 

`https://github.com/axllent/mailpit/releases/tag/v1.8.4`

If you have any issues, please check the generated `mailpit.log` file that will open up in this directory

Long story short, this should open up and smtp server on `localhost:1025`, and the UI for the email server should be viewable at `localhost:8025`.
However, there is a shutdown hook in the code so that when the code finishes the mailpit.exe shuts down automatically. If you want to view the ui after a test, remove the shutdown hook. If you do this, you have to manually kill the process by opening up task manager, and finding the mailpit.exe. A better way to view the UI is to debug the test, and put a breakpoint at the last line in the code and wait for it to hit that point. This should remove the need to remove the shutdown hook. 