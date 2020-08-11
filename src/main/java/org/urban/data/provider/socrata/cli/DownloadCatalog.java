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
package org.urban.data.provider.socrata.cli;

import java.io.File;
import java.io.IOException;
import org.urban.data.core.util.FileSystem;
import org.urban.data.provider.socrata.SocrataCatalog;
import org.urban.data.provider.socrata.db.DB;

/**
 * Download the current Socrata dataset catalog.
 * 
 * @author Heiko Mueller <heiko.mueller@nyu.edu>
 */
public class DownloadCatalog extends CommandImpl implements Command {

    public DownloadCatalog() {

        super("download catalog", "Download dataset catalog");
        this.addParameter(Args.PARA_OVERWRITE, "Overwrite existing catalog file (default: false)");
    }

    @Override
    public void run(Args args) throws IOException {
        
        DB db = args.getDB();
        String date = args.getDateDefaultToday();
        boolean overwrite = args.getOverwrite();
        System.out.println(date);
        System.out.println(overwrite);
        
        // Download the current Socrata catalog
        File catalogFile = db.catalogFile(date);
        if ((!catalogFile.exists()) || (overwrite)) {
            FileSystem.createParentFolder(catalogFile);
            new SocrataCatalog(catalogFile).download("dataset");
        }
    }
}
