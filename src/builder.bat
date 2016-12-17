@echo off

rem Assuming this script they are running this from their current structure
set SEMOSS_HOME=%CD%
echo SEMOSS_HOME is set to %SEMOSS_HOME%
echo:

if "%PROCESSOR_ARCHITECTURE%" == "AMD64" set FOLDER=x64
if "%PROCESSOR_ARCHITECTURE%" == "x86" set FOLDER=i386


Rem if the file exists.. all good go run semoss
if exist configured.txt (
goto runsemoss
)

Rem Ah yes, he is trying to configure this
if not exist configured.txt (
goto configureAll
)


Rem dont take chances, just go ahead and configure Java
set JAVA_HOME=%SEMOSS_HOME%/Java
set path=%path%;%SEMOSS_HOME%/Java/bin
set classpath=%classpath%;%SEMOSS_HOME%/tomcat/monolith/web-inf/classes


Rem overall configuration script
:configureAll
echo Running Configuration
echo =====================
echo:
set /p R_VAR="Do you have R installed? y/n>"
set CONFIG_TRY = "true"

if /I "%R_VAR%"=="n" GOTO installR

if /I "%R_VAR%"=="y" GOTO findR


Rem do a fresh installation of R
:installR
echo:
echo Configure - Installing R App 
echo ============================
echo:
rem semosshome/portables/R-portable/App/R-Portable/bin/x64
set R_HOME=%SEMOSS_HOME%/portables/R-Portable/App/R-Portable
set R_LIBS=%SEMOSS_HOME%/portables/R-Portable/App/R-portable/Library
set R_DLL_HOME=%SEMOSS_HOME%/portables/R-Portable/App/R-Portable/bin/%FOLDER%
echo R Portable installed in %R_HOME%
echo R_LIBS is available at %R_LIBS%
set JRI_HOME=%R_LIBS%/rJava/jri/%FOLDER%
echo JRI_HOME is %JRI_HOME%

echo:
echo:

GOTO configureSEMOSS

Rem Use an existing installation of R
:findR
echo Configure - Existing R App
echo ==========================
echo:
set /p R_HOME="Please specify your R Installation Directory"
set /p R_LIBS="Please enter the location of your R libraries use .libPaths() on R to find"

if exist %R_LIBS%/rJava echo found rJava

if not exist %R_LIBS%/rJava (
echo:
echo SEMOSS Requires rJava to be installed on your R
echo "install.packages('rJava', repos='http://cran.us.r-project.org');"
echo:
)

GOTO configureSEMOSS

Rem - configure SEMOSS i.e. all the javascript and such
:configureSEMOSS
echo Configure - SEMOSS
echo ==================
echo:
set classpath=%classpath%;%SEMOSS_HOME%/tomcat/monolith/web-inf/classes
Rem Semoss home, r home, R lib home, r dll home, jri home - 5 arguments 
call java prerna.configure.me %SEMOSS_HOME% %R_HOME% %R_LIBS% %R_DLL_HOME% %JRI_HOME%
echo:
echo:
goto runsemoss

:runsemoss
echo Executing - SEMOSS
echo ==================
echo:

if not exist configured.txt (
echo SEMOSS is not configured. 
if exist failed.txt (
echo We tried everything we can to configure your system. 
echo If you think you had made a mistake, please rerun this script.
echo else Please send failed.txt to contact semossinfo@gmail.com.
EXIT /B
)
EXIT /B
)

call setPath.bat
echo add tomcat file call here

echo:
rem semosshome/tomcat/bin
rem semosshome/tomcat/monolith/web-inf/classes

rem semosshome/portables/R-portable/App/R-Portable/bin/x64
rem semosshome/portable/R-portable/app/R-portable/library/rJava/jri/x64

