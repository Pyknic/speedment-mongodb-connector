/**
 *
 * Copyright (c) 2006-2015, Speedment, Inc. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); You may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at:
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package com.speedment.connector.mongodb;

import com.mongodb.MongoClient;
import com.mongodb.MongoCredential;
import static com.mongodb.MongoCredential.createCredential;
import com.mongodb.ServerAddress;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.speedment.core.config.model.Column;
import com.speedment.core.config.model.Dbms;
import com.speedment.core.config.model.PrimaryKeyColumn;
import com.speedment.core.config.model.Schema;
import com.speedment.core.config.model.Table;
import com.speedment.core.db.AsynchronousQueryResult;
import com.speedment.core.db.DbmsHandler;
import com.speedment.core.db.impl.SqlFunction;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;
import static java.util.Objects.requireNonNull;
import java.util.Optional;
import java.util.function.Consumer;
import java.util.function.Function;
import static java.util.function.Function.identity;
import static java.util.stream.Collectors.toMap;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;
import org.bson.Document;
import org.bson.types.ObjectId;

/**
 *
 * @author Emil Forslund
 */
public class MongoDbmsHandler implements DbmsHandler {
    
    private MongoClient client;
    private final Dbms dbms;
    
    private final static String PK = "_id";
    private final static String DEFAULT_HOST = "localhost";
    private final static boolean EXECUTE_IN_PARALLEL = true;
    
    public MongoDbmsHandler(Dbms dbms) {
        this.dbms = requireNonNull(dbms);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Dbms getDbms() {
        return dbms;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Stream<Schema> schemasUnpopulated() {
        return schemaNames(getMongoDatabase())
            .map(this::schemaFromName);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Stream<Schema> schemas() {
        return populatedSchemas(getMongoDatabase());
    }

    @Override
    public <T> Stream<T> executeQuery(String sql, List<?> values, SqlFunction<ResultSet, T> rsMapper) {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public <T> AsynchronousQueryResult<T> executeQueryAsync(String sql, List<?> values, Function<ResultSet, T> rsMapper) {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public void executeUpdate(String sql, List<?> values, Consumer<List<Long>> generatedKeyConsumer) throws SQLException {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }
    
    private MongoDatabase getMongoDatabase() {
        return getMongoClient().getDatabase(dbms.getName());
    }
    
    private MongoClient getMongoClient() {
        if (client == null) {
            final ServerAddress address = new ServerAddress(
                dbms.getIpAddress().orElse(DEFAULT_HOST), 
                dbms.getPort().orElse(MongoDbmsType.MONGODB.getDefaultPort())
            );

            final List<MongoCredential> credentials = new ArrayList<>();

            if (dbms.getUsername().isPresent()
            &&  dbms.getPassword().isPresent()) {
                credentials.add(createCredential(
                    dbms.getUsername().get(), 
                    dbms.getName(),
                    dbms.getPassword().get().toCharArray()
                ));
            }

            client = new MongoClient(address, credentials);
        }

        return client;
    }
    
    private Stream<String> schemaNames(MongoDatabase mongo) {
        return collectionNames(mongo)
            .map(this::collectionNameToSchemaName)
            .distinct();
    }

    private Stream<Schema> populatedSchemas(MongoDatabase mongo) {
        final Map<String, Schema> schemas = schemasUnpopulated()
            .collect(toMap(Schema::getName, identity()));
        
        return collectionNames(mongo)
            
            .map(fullName -> {
                final String schemaName = collectionNameToSchemaName(fullName);
                final Schema schema     = schemas.get(schemaName);
                
                final Optional<String> tableName = collectionNameToTableName(fullName);
                
                if (tableName.isPresent()) {
                    
                    final Table table = schema.addNewTable();
                    table.setName(tableName.get());
                    table.setTableName(tableName.get());

                    final MongoCollection<Document> col = mongo.getCollection(fullName);
                    final Document doc = col.find().first();
                    
                    doc.entrySet().forEach(entry -> {
                        final Column column = table.addNewColumn();
                        
                        column.setName(entry.getKey());
                        column.setNullable(true);
                        column.setMapping(determineMapping(doc, entry.getKey()));
                        
                        if (PK.equals(entry.getKey())) {
                            final PrimaryKeyColumn pk = table.addNewPrimaryKeyColumn();
                            pk.setName(PK);
                        }
                    });
                }
                
                return schema;
            })
            
            .distinct();
    }
    
    private Class<?> determineMapping(Document doc, String field) {
        try {
            final Boolean val = doc.getBoolean(field);
            return Boolean.class;
        } catch (ClassCastException ex) {}
        
        try {
            final Date val = doc.getDate(field);
            return Date.class;
        } catch (ClassCastException ex) {}
        
        try {
            final Double val = doc.getDouble(field);
            return Double.class;
        } catch (ClassCastException ex) {}
        
        try {
            final Integer val = doc.getInteger(field);
            return Integer.class;
        } catch (ClassCastException ex) {}
        
        try {
            final Long val = doc.getLong(field);
            return Long.class;
        } catch (ClassCastException ex) {}
        
        try {
            final ObjectId val = doc.getObjectId(field);
            return ObjectId.class;
        } catch (ClassCastException ex) {}
        
        try {
            final String val = doc.getString(field);
            return String.class;
        } catch (ClassCastException ex) {}
        
        final Object obj = doc.get(field);
        if (obj != null) {
            return obj.getClass();
        } else {
            return Object.class;
        }
    }
    
    private Stream<String> collectionNames(MongoDatabase mongo) {
        return StreamSupport.stream(mongo
                .listCollectionNames()
                .spliterator(),
            EXECUTE_IN_PARALLEL
        );
    }

    private String collectionNameToSchemaName(String collectionName) {
        if (collectionName.contains(".")) {
            return collectionName.substring(0, collectionName.indexOf("."));
        } else {
            return collectionName;
        }
    }
    
    private Optional<String> collectionNameToTableName(String collectionName) {
        if (collectionName.contains(".")) {
            return Optional.of(collectionName
                .substring(collectionName.indexOf(".") + 1)
                .replace(".", "_")
            );
        } else {
            return Optional.empty();
        }
    }
    
    private Schema schemaFromName(String name) {
        final Schema schema = Schema.newSchema();

        schema.setName(name);
        schema.setSchemaName(name);
        schema.setCatalogName(name);

        return schema;
    }
}