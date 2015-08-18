/*
 * Copyright 2013-2015 Qubole
 * Copyright 2013-2015 Makoto YUI
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.hive.jdbc.storagehandler;

import org.apache.hadoop.conf.Configuration;

public final class Constants {

    public static final String LIST_COLUMNS = "columns";
    public static final String LIST_COLUMN_TYPES = "columns.types";
    public static final String LAZY_SPLIT = "mapred.jdbc.hive.lazy.split";
    public static final String PREDICATE_REQUIRED = "jdbc.storage.handler.predicate.required";
    public static final String INPUT_FETCH_SIZE = "jdbc.storage.handler.input.fetch.size";

    public static final int DEFAULT_INPUT_FETCH_SIZE = 1000;
    private Constants() {
    }

    public static int getInputFetchSize(Configuration conf) {
        return conf.getInt(INPUT_FETCH_SIZE, 10000);
    }

}
