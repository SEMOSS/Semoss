artifact=monolith-0.0.1-SNAPSHOT

# Install war
cd war
rm -rf $artifact
mvn clean install
cd ..