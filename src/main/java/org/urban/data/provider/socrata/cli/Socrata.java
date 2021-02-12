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

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.logging.Level;
import java.util.logging.Logger;
import static org.urban.data.provider.socrata.cli.Args.PARA_BASEDIR;
import static org.urban.data.provider.socrata.cli.Args.PARA_CLEAN;
import static org.urban.data.provider.socrata.cli.Args.PARA_COLUMN;
import static org.urban.data.provider.socrata.cli.Args.PARA_DATASET;
import static org.urban.data.provider.socrata.cli.Args.PARA_DATE;
import static org.urban.data.provider.socrata.cli.Args.PARA_DOMAIN;
import static org.urban.data.provider.socrata.cli.Args.PARA_EXISTING;
import static org.urban.data.provider.socrata.cli.Args.PARA_HTML;
import static org.urban.data.provider.socrata.cli.Args.PARA_ORDERBY;
import static org.urban.data.provider.socrata.cli.Args.PARA_OUTPUT;
import static org.urban.data.provider.socrata.cli.Args.PARA_OVERWRITE;
import static org.urban.data.provider.socrata.cli.Args.PARA_REPORT;
import static org.urban.data.provider.socrata.cli.Args.PARA_REVERSE;
import static org.urban.data.provider.socrata.cli.Args.PARA_STATS;
import static org.urban.data.provider.socrata.cli.Args.PARA_THREADS;

/**
 * Socrata command line interface.
 * 
 * @author Heiko Mueller <heiko.mueller@nyu.edu>
 */
public class Socrata {
    
    private static final Command[] COMMANDS = {
        new Clean(),
        new ColumnFinder(),
        new ColumnValues(),
        new DomainNames(),
        new DownloadCatalog(),
        new DownloadDates(),
        new DownloadDatasets(),
        new DiskUsage(),
        new DatasetLoadfile(),
        new DatasetNames(),
        new DatasetSchema(),
        new DatabaseSnapshot(),
        new ExportColumnTypes(),
        new Parse()
    };

    private static final Logger LOGGER = Logger
        .getLogger(Socrata.class.getName());
    
    private static final String[] PARAMETERS = {
        PARA_BASEDIR,
        PARA_CLEAN,
        PARA_DOMAIN,
        PARA_DATASET,
        PARA_DATE,
        PARA_COLUMN,
        PARA_OUTPUT,
        PARA_EXISTING,
        PARA_HTML,
        PARA_ORDERBY,
        PARA_OVERWRITE,
        PARA_REPORT,
        PARA_REVERSE,
        PARA_STATS,
        PARA_THREADS
    };
    
    private static final String VERSION = "0.1.8";
    
    private static HashMap<String, Command> commandListing() {

        HashMap<String, Command> result = new HashMap<>();
        for (Command cmd : COMMANDS) {
            result.put(cmd.name(), cmd);
        }
        return result;
    }
    
    private static void printHelp() {
        
        ArrayList<Command> commands = new ArrayList<>(commandListing().values());
        Collections.sort(commands, (cmd1, cmd2) -> (cmd1.name().compareTo(cmd2.name())));
        Socrata.printProgramName();
        System.out.println();
        
        int maxLength = 0;
        for (Command cmd : commands) {
            if (cmd.name().length() > maxLength) {
                maxLength = cmd.name().length();
            }
        }
        for (Command cmd : commands) {
            System.out.println(
                    String.format(
                            "%1$-" + maxLength + "s : %2$s",
                            cmd.name(),
                            cmd.shortDescription()
                    )
            );
        }
    }
    
    private static void printHelp(Command cmd) {
        
        Socrata.printProgramName();
        System.out.println();

        int maxLength = cmd.name().length();
        for (String key : cmd.parameters().keySet()) {
            int len = key.length() + 2;
            if (len > maxLength) {
                maxLength = len;
            }
        }
        
        System.out.println(
                String.format(
                        "%1$-" + maxLength + "s : %2$s",
                        cmd.name(),
                        cmd.shortDescription()
                )
        );
        
        String description = cmd.longDescription();
        if (description != null) {
            System.out.println();
            System.out.println(description);
        }
        System.out.println();
        
        for (String key : PARAMETERS) {
            if (!cmd.parameters().containsKey(key)) {
                continue;
            }
            System.out.println(
                    String.format(
                            "--%1$-" + (maxLength - 2) + "s : %2$s",
                            key,
                            cmd.parameters().get(key))
            );
        }
    }
    
    private static void printProgramName() {
        
        System.out.println("Socrata Data Archive - Command line Tool (Version " + VERSION + ")\n");
    }

    public static void main(String[] arguments) {

        if (arguments.length == 0) {
            printHelp();
            System.exit(-1);
        }
        
        Socrata.printProgramName();
        
        Args args = new Args(arguments);
        
        Command command = commandListing().get(args.command());
        if (command == null) {
            System.out.println("Unknown command: " + args.command());
            System.exit(-1);
        }
        
        if (args.hasHelp()) {
            if (args.getHelp()) {
                Socrata.printHelp(command);
                System.exit(0);
            }
        }
        
        try {
            command.run(args);
        } catch (java.io.IOException ex) {
            LOGGER.log(Level.SEVERE, "RUN", ex);
            System.exit(-1);
        }        
    }
}
