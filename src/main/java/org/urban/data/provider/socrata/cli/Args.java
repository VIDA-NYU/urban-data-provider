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
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import org.urban.data.provider.socrata.archive.DB;

/**
 * Helper class for default command line arguments of archive management tools.
 * 
 * @author Heiko Mueller <heiko.mueller@nyu.edu>
 */
public class Args {
    
    /**
     * Environment variables
     */
    public static final String ENV_DATABASEDIR = "SOCRATA_DBDIR";
    public static final String ENV_THREADS = "SOCRATA_THREADS";
    
    /**
     * Command line parameter options
     */
    public final static String PARA_BASEDIR = "dir";
    public final static String PARA_DATASET = "dataset";
    public final static String PARA_DATE = "date";
    public final static String PARA_DOMAIN = "domain";
    public final static String PARA_HELP = "help";
    public final static String PARA_HTML = "html";
    public final static String PARA_OUTPUT = "output";
    public final static String PARA_REPORT = "report";
    public final static String PARA_THREADS = "threads";
    private final static HashSet<String> PARAMETERS = new HashSet<>(
            Arrays.asList(new String[]{
                PARA_BASEDIR,
                PARA_DATASET,
                PARA_DATE,
                PARA_DOMAIN,
                PARA_HELP,
                PARA_HTML,
                PARA_OUTPUT,
                PARA_REPORT,
                PARA_THREADS
            })
    );
    
    private final String _command;
    private final HashMap<String, String> _parameters;
    
    public Args(String[] args) {

        _command = args[0];
        _parameters = new HashMap<>();
        
        for (int iArg = 1; iArg < args.length; iArg++) {
            String arg = args[iArg];
            if (arg.startsWith("--")) {
                String para = arg.substring(2);
                String key;
                String value;
                if (para.contains("=")) {
                    int pos = para.indexOf("=");
                    key = para.substring(0, pos).trim().toLowerCase();
                    value = para.substring(pos + 1).trim();
                } else {
                    key = para.toLowerCase();
                    value = Boolean.toString(true);
                }
                if (PARAMETERS.contains(key)) {
                    _parameters.put(key, value);
                } else {
                    throw new IllegalArgumentException(arg);
                }
            } else {
                throw new IllegalArgumentException(arg);
            }
        }
    }
    
    public String command() {
    
        return _command;
    }
    
    public String getDataset() {
        
        return _parameters.get(PARA_DATASET);
    }
    
    public DB getDB() {
        
        return new DB(this.getDirectory());
    }
    
    public File getDirectory() {
        
        if (_parameters.containsKey(PARA_BASEDIR)) {
            return new File(_parameters.get(PARA_BASEDIR));
        } else {
            Map<String, String> env = System.getenv();
            if (env.containsKey(PARA_BASEDIR)) {
                return new File(env.get(PARA_BASEDIR));
            }
        }
        return new File(".");
    }
    
    /**
     * Get the value of the date parameter. If the parameter was not given the
     * current date is returned as the default value;
     * @return 
     */
    public String getDate() {
        
        if (this.hasDate()) {
            return _parameters.get(PARA_DATE);
        } else {
            return DB.DF.format(new Date());
        }
    }
    
    public String getDomain() {
        
        return _parameters.get(PARA_DOMAIN);
    }
    
    public boolean getHelp() {
        
        return Boolean.parseBoolean(_parameters.get(PARA_HELP));
    }
    
    public boolean getHtml() {
        
        if (this.hasHtml()) {
            return Boolean.parseBoolean(_parameters.get(PARA_HTML));
        } else {
            return false;
        }
    }
    
    public boolean getReport() {
        
        if (this.hasReport()) {
            return Boolean.parseBoolean(_parameters.get(PARA_REPORT));
        } else {
            return false;
        }
    }
    
    /**
     * Get the value for the number of threads. If the threads parameter was
     * given this value is returned. Otherwise, we first try to get he value
     * from the respective environment variable. If the variable is not set
     * the default value is returned. The default is currently set to 6.
     * 
     * @return 
     */
    public int getThreads() {
        
        String val;
        if (_parameters.containsKey(PARA_THREADS)) {
            val = _parameters.get(PARA_THREADS);
        } else {
            val = System.getenv().get(ENV_THREADS);
        }
        if (val != null) {
            try {
                return Integer.parseInt(val);
            } catch (java.lang.NumberFormatException ex) {
            }
        }
        return 6;
    }
    
    public File getOutput() {
        
        return new File(_parameters.get(PARA_OUTPUT));
    }
    
    public boolean hasDataset() {
        
        return _parameters.containsKey(PARA_DATASET);
    }
    
    public boolean hasDate() {
        
        return _parameters.containsKey(PARA_DATE);
    }
    
    public boolean hasDomain() {
        
        return _parameters.containsKey(PARA_DOMAIN);
    }
    
    public boolean hasHelp() {
        
        return _parameters.containsKey(PARA_HELP);
    }
    
    public boolean hasHtml() {
        
        return _parameters.containsKey(PARA_HTML);
    }
    
    public boolean hasOutput() {
        
        return _parameters.containsKey(PARA_OUTPUT);
    }
    
    public boolean hasReport() {
        
        return _parameters.containsKey(PARA_REPORT);
    }
}
