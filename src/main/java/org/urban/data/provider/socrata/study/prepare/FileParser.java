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
package org.urban.data.provider.socrata.study.prepare;

import java.io.BufferedReader;
import java.io.File;
import java.io.InputStream;
import java.io.InputStreamReader;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.urban.data.core.io.FileSystem;

/**
 *
 * @author Heiko Mueller <heiko.mueller@nyu.edu>
 */
public class FileParser {
    
    public static void main(String[] args) {
        
        if (args.length != 2) {
            System.out.println("Usage: <input-file-list> <spilt-index>");
            System.exit(-1);
        }
        
        File inputFile = new File(args[0]);
        int index = Integer.parseInt(args[1]);
        
        try (BufferedReader in = FileSystem.openReader(inputFile)) {
            String line;
            while ((line = in.readLine()) != null) {
                String filename = line.split("\\s")[index];
                new FileParser().parse(new File(filename));
            }
        } catch (java.io.IOException ex) {
            ex.printStackTrace();
            System.exit(-1);
        }
    }
    
    public void parse(File file) throws java.io.IOException {
        
        System.out.println(file.getAbsolutePath());
        try (InputStream is = FileSystem.openFile(file)) {
            CSVParser parser;
            parser = new CSVParser(new InputStreamReader(is), CSVFormat.TDF);
            int lineCount = 0;
            for (CSVRecord row : parser) {
                lineCount++;
                if (row.size() != 2) {
                    System.out.println(lineCount + "\t" + row.size());
                    System.out.println(row);
                }
            }
            System.out.println(lineCount);
        } catch (java.io.IOException ex) {
            ex.printStackTrace();
            return;
        }
    }
}
