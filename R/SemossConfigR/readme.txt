How to configure R for Semoss

1) Download R-Portable from:
https://sourceforge.net/projects/rportable/files/R-Portable/

2) Install R-Portable using the downloaded executable file

3) Place the SemossConfigR folder into R-Portable (<some path>/R-Portable/SemossConfigR)

4) Run R-Portable/SemossConfigR/configureR-Portable.bat
(Wait for the popup to say "R-Portable is now configured for Semoss!" before closing)

This will:
	a) Replace R-Portable/App/R-Portable/etc/Rprofile.site so that R-Portable installs packages locally in R-Portable/App/R-Portable/library
	b) Install the packages registered in SemossConfigR/scripts/Packages.R
	c) Create R-Portable/startRserve.bat

Dev note:

Additional packages can be registered in SemossConfigR/scripts/Packages.R