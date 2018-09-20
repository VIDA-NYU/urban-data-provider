/*
 * Copyright 2018 New York University.
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
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Download part of the Socrata resource catalog.
 * 
 * Downloads information for all resources of given type in a given domain.
 * 
 * @author Heiko Mueller <heiko.mueller@nyu.edu>
 */
public class SocrataCatalogDownload {
    
    private static final String COMMAND =
            "Usage: <catalog-file> <type> {<domain>}";
    
    private static final Logger LOGGER = Logger.getGlobal();
    
    public static void main(String[] args) {
        
        if ((args.length < 2) || (args.length > 3)) {
            System.out.println(COMMAND);
            System.exit(-1);
        }
        
        File outputFile = new File(args[0]);
        String type = args[1];
        String domain = null;
        if (args.length == 3) {
            domain = args[2];
        }
        
        SocrataCatalog catalog = new SocrataCatalog(outputFile);
        try {
            if (domain != null) {
                catalog.download(domain, type);
            } else {
                catalog.download(type);
            }
        } catch (java.net.URISyntaxException | java.io.IOException ex) {
            LOGGER.log(Level.SEVERE, "RUN", ex);
        }
    }
}
