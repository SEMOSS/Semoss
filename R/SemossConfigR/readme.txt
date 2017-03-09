How to configure R for SEMOSS

1) Download R-Portable from:
https://sourceforge.net/projects/rportable/files/R-Portable/

2) Install R-Portable using the downloaded executable file

3) Place the SemossConfigR folder into R-Portable (<some path>/R-Portable/SemossConfigR)

4) Run R-Portable/SemossConfigR/configureR-Portable.bat
(Wait for the popup to say "R-Portable is now configured for SEMOSS!" before closing)

This will:
	a) Replace R-Portable/App/R-Portable/etc/Rprofile.site so that R-Portable installs packages locally in R-Portable/App/R-Portable/library
	b) Install the packages registered in SemossConfigR/scripts/Packages.R
	c) Create R-Portable/startRserve.bat

Dev note:

Additional packages can be registered in SemossConfigR/scripts/Packages.R

5) For JRI: Run R-Portable/SemossConfigR/setREnvVars.bat

This will set the necessary user (not system) environment variables for JRI

Note:
	If you get the following warning:
		WARNING: The data being saved is truncated to 1024 characters.
	Then you must edit the PATH manually
	Just add the values of R_DLL_HOME and JRI_HOME into your PATH variable
	Usually when I get this warning, both have been added to the PATH,
	but one has been truncated, so I need to replace it with the full value

6) Running R for SEMOSS: You can either use Rserve or JRI
	a) Rserve: 
		i) Launch R-Portable\startRserve.bat
		ii) Search for R_CONNECTION_JRI in RDF_Map.prop, set to false, and restart SEMOSS
	b) JRI:
		i) Make sure the environment variables are set for JRI (step 5)
		ii) Restart Eclipse
		iii) Set R_CONNECTION_JRI in RDF_Map.prop to true, and start SEMOSS (no need to launch startRserve.bat)
