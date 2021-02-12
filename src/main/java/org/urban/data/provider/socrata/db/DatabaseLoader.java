/*
 * Copyright 2019 New York University.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.urban.data.provider.socrata.db;

import java.io.File;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.List;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.urban.data.core.util.FileSystem;
import org.urban.data.core.util.StringHelper;
import org.urban.data.provider.socrata.profiling.ColumnStats;
import org.urban.data.core.util.Counter;

/**
 * Collection of classes to create database load files from downloaded dataset
 * files.
 * 
 * @author Heiko Mueller <heiko.mueller@nyu.edu>
 */
public class DatabaseLoader {
   
    private final DB _db;
    private final int _valueLengthThreshold;
    
    public DatabaseLoader(DB db, int valueLengthThreshold) {
        
        _db = db;
        _valueLengthThreshold = valueLengthThreshold;
    }

    public DatabaseLoader(DB db) {
        
        this(db, 1024);
    }
    
    public void createLoadFile(Dataset dataset, File outputDir) throws java.io.IOException {
            
        File loadFile = FileSystem.joinPath(outputDir, dataset.identifier() + ".data");
        Counter truncCount = new Counter();
        
        List<ColumnStats> schema = null;
        
        try (
                CSVParser in = _db.open(dataset);
                PrintWriter out = FileSystem.openPrintWriter(loadFile)
        ) {
            schema = new ArrayList<>();
            for (String name : in.getHeaderNames()) {
                name = DatabaseLoader.replaceSpecialChars(name);
                // If the name starts with a digit add an underscore
                if (Character.isDigit(name.charAt(0))) {
                    name = "_" + name;
                }
                schema.add(new ColumnStats(name));
            }            
            for (CSVRecord record : in) {
                int count = 0;
                String line = null;
                for (String value : record) {
                    value = this.escapeTerm(value, truncCount);
                    schema.get(count).add(value);
                    if (line == null) {
                        line = value;
                    } else {
                        line += "\t" + value;
                    }
                    count++;
                }
                for (int iCol = count; iCol < schema.size(); iCol++) {
                    schema.get(iCol).add(null);
                    if (line != null) {
                        line += "\t";
                    }
                }
                if (line != null) {
                    out.println(line);
                }
            }
        }

        String tableName = "ds_" + dataset.identifier().replaceAll("-", "_");
        
        File scriptFile = FileSystem.joinPath(outputDir, dataset.identifier() + ".sql");
        try (PrintWriter out = FileSystem.openPrintWriter(scriptFile)) {
            out.println("--");
            out.println("-- Drop table for dataset " + dataset.domain() + "." + dataset.identifier());
            out.println("--");
            out.println("DROP TABLE IF EXISTS " + tableName + ";\n");
            out.println("--");
            out.println("-- Create table for dataset " + dataset.domain() + "." + dataset.identifier());
            out.println("--");
            out.println("CREATE TABLE " + tableName + "(");
            ArrayList<String> names = new ArrayList<>();
            for (int iCol = 0; iCol < schema.size(); iCol++) {
                ColumnStats col = schema.get(iCol);
                String sql = col.sqlStmt();
                if (iCol < schema.size() - 1) {
                    sql += ",";
                }
                out.println("  " + sql);
                names.add(col.name());
            }
            out.println(");\n");
            out.println("--");
            out.println("-- Load data for dataset " + dataset.domain() + "." + dataset.identifier());
            out.println("--");
            String columns = StringHelper.joinStrings(names, ",");
            out.println("\\copy " + tableName + "(" + columns + ") from './" + loadFile.getName() + "' with delimiter E'\\t' null ''\n");
        }
    }
    
    public String escapeTerm(String term, Counter truncCount) {
        
        String value = term;
        
        if (term.length() > _valueLengthThreshold) {
            value = value.substring(0, _valueLengthThreshold) + "__" + (truncCount.inc());
        }
        if (value.contains("\'")) {
            value = value.replaceAll("\'", "\'\'");
        }
        if (value.contains("\\")) {
            value = value.replaceAll("\\\\", "\\\\\\\\");
        }
        if (value.contains("\n")) {
            value = value.replaceAll("\\n", " ");
        }
        return value;
    }
    
    public static String replaceSpecialChars(String name) {
        
        return name.replaceAll("[^\\dA-Za-z]", "_");
    }
}
