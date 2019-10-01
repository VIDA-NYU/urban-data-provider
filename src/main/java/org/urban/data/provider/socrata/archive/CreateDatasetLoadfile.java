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
package org.urban.data.provider.socrata.archive;

import java.io.File;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Create load file for a given dataset.
 * 
 * @author Heiko Mueller <heiko.mueller@nyu.edu>
 */
public class CreateDatasetLoadfile {
   
    private final static String COMMAND =
            "Usage:\n" +
            "  {--domain=<domain>}\n" +
            "  {--date=<date>}\n" +
            "  {--dataset=<dataset>}\n" +
            "  <database-dir>\n" +
            "  <output-dir>";
    
    private final static Logger LOGGER = Logger
            .getLogger(CreateDatasetLoadfile.class.getName());
    
    public static void main(String[] args) {
        
        if (args.length != 5) {
            System.out.println(COMMAND);
            System.exit(-1);
        }
        
        Args cmd = new Args(args, COMMAND);
        
        Dataset dataset = new Dataset(cmd.getDataset(), cmd.getDomain(), cmd.getDate());
        DB db = new DB(new File(cmd.get(0)));
        File outputDir = new File(cmd.get(1));
        
        try {
            new DatabaseLoader(db).createLoadFile(dataset, outputDir);
        } catch (java.io.IOException ex) {
            LOGGER.log(Level.SEVERE, "RUN", ex);
            System.exit(-1);
        }
    }
}
