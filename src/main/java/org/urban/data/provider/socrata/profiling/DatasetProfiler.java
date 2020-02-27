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
package org.urban.data.provider.socrata.profiling;

import java.io.File;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.urban.data.provider.socrata.SocrataHelper;

/**
 * Data profiler for a Socrata dataset. Computes basic statistics about values
 * in the dataset columns.
 * 
 * @author Heiko Mueller <heiko.mueller@nyu.edu>
 */
public class DatasetProfiler {
    
    public void run(CSVParser in) {
        
        List<String> columnNames = in.getHeaderNames();
        
        ColumnProfiler[] columns = new ColumnProfiler[columnNames.size()];
        for (int iColumn = 0; iColumn < columnNames.size(); iColumn++) {
            columns[iColumn] = new ColumnProfiler(columnNames.get(iColumn));
        }
        
        for (CSVRecord row : in) {
            for (int iColumn = 0; iColumn < row.size(); iColumn++) {
                columns[iColumn].add(row.get(iColumn));
            }
        }
        
        for (ColumnProfiler column : columns) {
            System.out.println(column.name());
            System.out.println("  NON-EMPTY CELLS    : " + column.nonEmptyCells());
            System.out.println("  EMPTY CELLS        : " + column.emptyCells());
            System.out.println("  DISTINCT DATES     : " + column.distinctDateValues());
            System.out.println("  DISTINCT DECIMAL   : " + column.distinctDecimalValues());
            System.out.println("  DISTINCT GEO POINTS: " + column.distinctGeoValues());
            System.out.println("  DISTINCT INTEGER   : " + column.distinctIntValues());
            System.out.println("  DISTINCT LONG      : " + column.distinctLongValues());
            System.out.println("  DISTINCT TEXT      : " + column.distinctTextValues());
            System.out.println("  DISTINCT VALUES    : " + column.distinctValues());
            System.out.println();
        }
    }
    
    public static void main(String[] args) {
        
        if (args.length != 1) {
            System.out.println("Usage: <dataset-file>");
            System.exit(-1);
        }
        
        File datasetFile = new File(args[0]);
        
        try (CSVParser in = SocrataHelper.tsvParser(datasetFile)) {
            new DatasetProfiler().run(in);
        } catch (java.io.IOException ex) {
            Logger.getGlobal().log(Level.SEVERE, "RUN", ex);
            System.exit(-1);
        }
    }
}
