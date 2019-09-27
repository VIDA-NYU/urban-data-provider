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
package org.urban.data.provider.socrata;

import java.io.File;
import java.io.PrintWriter;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.urban.data.core.io.FileSystem;

/**
 * Print a list of available domains at the Socrata API catalogs to standard
 * output.
 * 
 * Output is tab-delimited with the first value being the domain name and the
 * second value the domain resource count.
 * 
 * If the optional output file parameter is given output is written to the file
 * instead of standard output.
 * 
 * @author Heiko Mueller <heiko.mueller@nyu.edu>
 */
public class SocrataDomains {
   
    public static final String COMMAND = 
            "Usage:\n" +
            "  {<output-file>}";
    
    public static final Logger LOGGER = Logger.getGlobal();
    
    public static final String VERSION = "0.1.0";
    
    public static void main(String[] args) {
        
        if (args.length > 1) {
            System.out.println(COMMAND);
            System.exit(-1);
        }
        
        PrintWriter out;
        try {
            if (args.length == 1) {
                out = FileSystem.openPrintWriter(new File(args[0]));
            } else {
                out = new PrintWriter(System.out);
            }
            for (SocrataDomain domain : SocrataCatalog.listDomains()) {
                out.println(domain.name() + "\t" + domain.count());
            }
            out.close();
        } catch (java.io.IOException ex) {
            LOGGER.log(Level.SEVERE, "RUN", ex);
            System.exit(-1);
        }
    }
}
