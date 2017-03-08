:: This batch file is meant to set R system environment variables for SEMOSS
:: Must be run from R-Portable\SemososConfigR\setREnvVars.bat 

:: Go up a level to R-Portable parent directory
@echo off
CD ..

:: Check whether the script is being executed within R-Portable
:: If not, terminate the script
FOR %%a IN (.) DO SET parent=%%~na
IF NOT %parent%==R-Portable GOTO NOPATH

:: Set variables

:: R_HOME
:: Must set locally first so that references within this script are up to date
SET R_HOME=%CD%/App/R-Portable
ECHO Setting R_HOME to %R_HOME%

:: Include quotes in case the path contains spaces
:: SETX delimits using spaces
SETX R_HOME "%R_HOME%"
ECHO.
ECHO ==================================================
ECHO.

:: R_LIBS
SET R_LIBS=%R_HOME%/library
ECHO Setting R_LIBS to %R_LIBS%
SETX R_LIBS "%R_LIBS%"
ECHO.
ECHO ==================================================
ECHO.

:: Determine whether to use 32-bit or 64-bit subfolders
IF "%PROCESSOR_ARCHITECTURE%" == "AMD64" SET FOLDER=x64
IF "%PROCESSOR_ARCHITECTURE%" == "x86" SET FOLDER=i386

:: R_DLL_HOME
SET R_DLL_HOME=%R_HOME%/bin/%FOLDER%
ECHO Setting R_DLL_HOME to %R_DLL_HOME%
SETX R_DLL_HOME "%R_DLL_HOME%"
ECHO.
ECHO ==================================================
ECHO.

:: JRI_HOME
SET JRI_HOME=%R_LIBS%/rJava/jri/%FOLDER%
ECHO Setting JRI_HOME to %JRI_HOME%
SETX JRI_HOME "%JRI_HOME%"
ECHO.
ECHO ==================================================
ECHO.

:: Include R_DLL_HOME and JRI_HOME in PATH for JRI

:: R_DLL_HOME to PATH
:: See http://superuser.com/questions/601015/how-to-update-the-path-user-environment-variable-from-command-line
ECHO Adding %R_DLL_HOME% to PATH
SET ok=0
FOR /f "skip=2 tokens=3*" %%a IN ('reg query HKCU\Environment /v PATH') DO IF [%%b]==[] ( SETX PATH "%%~a;%R_DLL_HOME%" && SET ok=1 ) else ( SETX PATH "%%~a %%~b;%R_DLL_HOME%" && set ok=1 )
IF "%ok%" == "0" SETX PATH "%R_DLL_HOME%"
ECHO.
ECHO ==================================================
ECHO.

:: JRI_HOME to PATH
ECHO Adding %JRI_HOME% to PATH
SET ok=0
FOR /f "skip=2 tokens=3*" %%a IN ('reg query HKCU\Environment /v PATH') DO IF [%%b]==[] ( SETX PATH "%%~a;%JRI_HOME%" && SET ok=1 ) else ( SETX PATH "%%~a %%~b;%JRI_HOME%" && set ok=1 )
IF "%ok%" == "0" SETX PATH "%JRI_HOME%"
ECHO.
ECHO If you recieved the following warning: WARNING: The data being saved is truncated to 1024 characters
ECHO (after adding %JRI_HOME% to PATH),
ECHO then you must edit the PATH to include the directories of R_DLL_HOME and JRI_HOME manually.
ECHO.
ECHO This occurs when appending to the PATH causes the PATH to exceed 1024 characters. The PATH can 
ECHO nonetheless exceed 1024 characters when edited manually.
ECHO.
ECHO Search for "edit the system environment variables" in Windows, click advanced, environment variables. 
ECHO From the list of user variables, find the value of PATH, highlight it, select edit, and check whether
ECHO either of these entries exist in full or in part in the list:
ECHO.
ECHO      a) R_DLL_HOME: (path to SEMOSS home)\portables\R-Portable/App/R-Portable/bin/x64
ECHO      b) JRI_HOME: (path to SEMOSS home)\portables\R-Portable/App/R-Portable/library/rJava/jri/x64
ECHO.
ECHO The PATH must be editied so that both (a) and (b) exist in full. To do this, find the values of 
ECHO R_DLL_HOME and JRI_HOME from the list of user variables (highlight, select edit, and copy the 
ECHO variable value), and edit the PATH so that it includes the values of both R_DLL_HOME and JRI_HOME.
ECHO.
ECHO If you did not receive the warning, then no action needs to be taken.
ECHO.
ECHO ==================================================
ECHO.

:: Complete
ECHO R environment variables are now set for SEMOSS!
GOTO END

:NOPATH
ECHO This script is not being executed from within R-Portable,
ECHO copy SemossConfigR into R-Portable and execute again
GOTO END

:END
PAUSE