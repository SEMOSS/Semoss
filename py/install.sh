echo 'Uninstalling and reinstalling jep version for SEMOSS'
pip uninstall Jep
pip install jep==3.9.0
echo 'Installing python libraries'
pip install pandas==0.25.3
pip install annoy
pip install numpy
pip install fuzzywuzzy
pip install random
pip install datetime
pip install pyjarowinkler
pip install xlrd