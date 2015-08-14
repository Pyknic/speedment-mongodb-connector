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

import com.speedment.core.config.model.Dbms;
import com.speedment.core.config.model.parameters.DbmsType;
import com.speedment.core.db.DbmsHandler;
import java.util.HashSet;
import java.util.Optional;
import java.util.Set;

/**
 *
 * @author Emil Forslund
 */
public enum MongoDbmsType implements DbmsType {
    
    MONGODB;

    @Override
    public String getName() {
        return "MongoDB";
    }

    @Override
    public String getDriverManagerName() {
        return "Speedment MongoDB Connector";
    }

    @Override
    public int getDefaultPort() {
        return 27017;
    }

    @Override
    public String getSchemaTableDelimiter() {
        return ".";
    }

    @Override
    public String getDbmsNameMeaning() {
        return "The DBMS name is not used in this implementation.";
    }

    @Override
    public boolean isSupported() {
        return true;
    }

    @Override
    public String getDriverName() {
        return MongoDbmsHandler.class.getName();
    }

    @Override
    public Optional<String> getDefaultConnectorParameters() {
        return Optional.empty();
    }

    @Override
    public String getJdbcConnectorName() {
        return "mongodb";
    }

    @Override
    public String getFieldEncloserStart(boolean isWithinQuotes) {
        if (isWithinQuotes) {
            return "\\\"";
        } else {
            return "\"";
        }
    }

    @Override
    public String getFieldEncloserEnd(boolean isWithinQuotes) {
        if (isWithinQuotes) {
            return "\\\"";
        } else {
            return "\"";
        }
    }

    @Override
    public Set<String> getSchemaExcludSet() {
        return new HashSet<>();
    }

    @Override
    public DbmsHandler makeDbmsHandler(Dbms dbms) {
        return new MongoDbmsHandler(dbms);
    }
}