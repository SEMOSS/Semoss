artifact=monolith-0.0.1-SNAPSHOT

# Install lib
cd lib
rm -rf $artifact
mvn clean install
cd ..