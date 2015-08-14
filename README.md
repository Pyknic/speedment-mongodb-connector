# Speedment MongoDB Connector
An extension to Speedment that allows you to connect to and generate sources from a MongoDB document structure.

### pom.xml
```xml
<dependencies>
    <!-- Add Speedment as a dependency -->
    <dependency>
        <groupId>com.speedment</groupId>
        <artifactId>speedment</artifactId>
        <version>${speedment.version}</version>
    </dependency>
</dependencies>
...
<build>
    <plugins>
        <plugin>
            <artifactId>speedment-maven-plugin</artifactId>
            <groupId>com.speedment</groupId>
            <version>${speedment.version}</version>

            <!-- Add the connector as a dependency to the plugin -->
            <dependencies>
                <dependency>
                    <groupId>com.speedment</groupId>
                    <artifactId>speedment-mongodb-connector</artifactId>
                    <version>${speedment.version}</version>
                </dependency>
            </dependencies>
            
            <!-- Tell the plugin to include the connector -->
            <configuration>
                <components>
                    <component implementation="com.speedment.connector.mongodb.MongoDbConnector"></component>
                </components>
            </configuration>
        </plugin>
    </plugins>
</build>
```
