:: This batch file is meant to configure R-Portable for SEMOSS
:: Must be run from R-Portable\SemososConfigR\configureR-Portable.bat 

:: Go up a level to R-Portable parent directory
@echo off
CD..

:: Check whether the script is being executed within R-Portable
:: If not, terminate the script
FOR %%a IN (.) DO SET parent=%%~na
IF NOT %parent%==R-Portable GOTO NOPATH

:: Copy scripts into parent directory
MD scripts
XCOPY /Y SemossConfigR\scripts scripts

:: Replace the default Rprofile.site file with the custom one
:: Sets the default CRAN mirror
:: and ensures that all packages are installed into the local library
XCOPY /Y SemossConfigR\Rprofile.site App\R-Portable\etc\Rprofile.site

:: Install packages
App\R-Portable\bin\Rscript.exe scripts\Packages.R

:: Create a batch script to start Rserve
@echo App\R-Portable\bin\Rscript.exe scripts\Rserve.R > "startRserve.bat"

:: Configuration completed
@echo R-Portable has already been configured for Semoss! > "rportableconfigured.txt"
ECHO ==================================================
ECHO R-Portable is now configured for Semoss!
GOTO END

:NOPATH
ECHO This script is not being executed from within R-Portable,
ECHO copy SemossConfigR into R-Portable and execute again
GOTO END

:END
PAUSE