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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import org.urban.data.core.set.StringSet;
import org.urban.data.core.util.StringHelper;
import org.urban.data.provider.socrata.db.DB;
import org.urban.data.provider.socrata.db.DatasetQuery;

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
    public static final String ENV_DOMAIN = "SOCRATA_DOMAIN";
    public static final String ENV_THREADS = "SOCRATA_THREADS";
    
    /**
     * Argument value for order by parameter
     */
    public static final String ORDER_BY_COUNT = "count";
    public static final String ORDER_BY_VALUE = "value";
    
    /**
     * Command line parameter options
     */
    public final static String PARA_BASEDIR = "dir";
    public final static String PARA_CLEAN = "clean";
    public final static String PARA_COLUMN = "column";
    public final static String PARA_DATASET = "dataset";
    public final static String PARA_DATE = "date";
    public final static String PARA_DOMAIN = "domain";
    public final static String PARA_EXISTING = "existing";
    public final static String PARA_HELP = "help";
    public final static String PARA_HTML = "html";
    public final static String PARA_ORDERBY = "orderby";
    public final static String PARA_OUTPUT = "output";
    public final static String PARA_OVERWRITE = "overwrite";
    public final static String PARA_REPORT = "report";
    public final static String PARA_REVERSE = "reverse";
    public final static String PARA_STATS = "stats";
    public final static String PARA_THREADS = "threads";
    public final static String PARA_VALUES = "values";
    private final static HashSet<String> PARAMETERS = new HashSet<>(
            Arrays.asList(new String[]{
                PARA_BASEDIR,
                PARA_CLEAN,
                PARA_COLUMN,
                PARA_DATASET,
                PARA_DATE,
                PARA_DOMAIN,
                PARA_EXISTING,
                PARA_HELP,
                PARA_HTML,
                PARA_ORDERBY,
                PARA_OUTPUT,
                PARA_OVERWRITE,
                PARA_REPORT,
                PARA_REVERSE,
                PARA_STATS,
                PARA_THREADS,
                PARA_VALUES
            })
    );
    
    private final String _command;
    private final HashMap<String, String> _parameters;
    
    public Args(String[] args) {

        ArrayList<String> tokens = new ArrayList<>();
        int argIndex = 0;
        while (!args[argIndex].startsWith("--")) {
            tokens.add(args[argIndex++]);
            if (argIndex >= args.length) {
                break;
            }
        }
        _command = StringHelper.joinStrings(tokens, " ");

        _parameters = new HashMap<>();        
        for (int iArg = argIndex; iArg < args.length; iArg++) {
            String arg = args[iArg];
            if (arg.startsWith("--")) {
                String para = arg.substring(2);
                String key;
                String value;
                if (para.contains("=")) {
                    int pos = para.indexOf("=");
                    key = para.substring(0, pos).trim().toLowerCase();
                    value = para.substring(pos + 1).trim();
                    if (value.equals("")) {
                        value = null;
                    }
                } else {
                    key = para.toLowerCase().toLowerCase();
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
    
    public void add(String key, String value) {
    
    	_parameters.put(key,  value);
    }
    
    public DatasetQuery asQuery() {
        
        return new DatasetQuery()
                .domain(this.getDomain())
                .date(this.getDate())
                .dataset(this.getDataset());
    }
    
    public String command() {
    
        return _command;
    }
    
    public boolean getClean() {
        
        if (_parameters.containsKey(PARA_CLEAN)) {
            return Boolean.parseBoolean(_parameters.get(PARA_CLEAN));
        } else {
            return false;
        }
    }

    public String getColumn() {
        
        return _parameters.get(PARA_COLUMN);
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
     * Get the value of the date parameter. The result is null if the parameter
     * was not given.
     * 
     * @return 
     */
    public String getDate() {
        
        return _parameters.get(PARA_DATE);
    }
    
    /**
     * Get the value of the date parameter. If the parameter was not given the
     * current date is returned as the default value.
     * 
     * @return 
     */
    public String getDateDefaultToday() {
        
        if (this.hasDate()) {
            return _parameters.get(PARA_DATE);
        } else {
            return DB.DF.format(new Date());
        }
    }
    
    /**
     * Get the value of the date parameter. If the parameter was not given the
     * last download date is returned as the default value.
     * 
     * @return 
     */
    public String getDateDefaultLast() {
        
        if (this.hasDate()) {
            return _parameters.get(PARA_DATE);
        } else {
            return this.getDB().lastDownloadDate();
        }
    }
    
    /**
     * Get the value for the domain parameter. If the parameter is not set
     * the value from the respective environment variable is returned. If
     * neither value is set the result is null.
     * 
     * @return 
     */
    public String getDomain() {
        
        if (_parameters.containsKey(PARA_DOMAIN)) {
            return _parameters.get(PARA_DOMAIN);
        } else {
            String val = System.getenv().get(ENV_DOMAIN);
            if (val != null) {
                if (!val.trim().equals("")) {
                    return val.trim();
                }
            }
        }
        return null;
    }
    
    public boolean getExisting() {
        
        if (_parameters.containsKey(PARA_EXISTING)) {
            return Boolean.parseBoolean(_parameters.get(PARA_EXISTING));
        } else {
            return false;
        }
    }
    
    public boolean getHelp() {
        
        return Boolean.parseBoolean(_parameters.get(PARA_HELP));
    }
    
    public boolean getHtml() {
        
        if (_parameters.containsKey(PARA_HTML)) {
            return Boolean.parseBoolean(_parameters.get(PARA_HTML));
        } else {
            return false;
        }
    }
    
    public boolean getOverwrite() {
        
        if (_parameters.containsKey(PARA_OVERWRITE)) {
            return Boolean.parseBoolean(_parameters.get(PARA_OVERWRITE));
        } else {
            return false;
        }
    }
   
    public boolean getReport() {
        
        if (_parameters.containsKey(PARA_REPORT)) {
            return Boolean.parseBoolean(_parameters.get(PARA_REPORT));
        } else {
            return false;
        }
    }
    
    public boolean getReverse() {
        
        if (_parameters.containsKey(PARA_REVERSE)) {
            return Boolean.parseBoolean(_parameters.get(PARA_REVERSE));
        } else {
            return false;
        }
    }
    
    public boolean getStatsOnly() {
        
        if (_parameters.containsKey(PARA_STATS)) {
            return Boolean.parseBoolean(_parameters.get(PARA_STATS));
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
    
    public String getOrderBy() {
        
        if (_parameters.containsKey(PARA_ORDERBY)) {
            return _parameters.get(PARA_ORDERBY);
        } else {
            return ORDER_BY_VALUE;
        }
    }
    
    public File getOutput() {
        
        return new File(_parameters.get(PARA_OUTPUT));
    }
    
    public StringSet getValues() {
        
        return new StringSet(_parameters.get(PARA_VALUES).split(","));
    }
    
    public boolean hasColumn() {
        
        return _parameters.containsKey(PARA_COLUMN);
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
    
    public boolean hasOutput() {
        
        return _parameters.containsKey(PARA_OUTPUT);
    }
    
    public boolean hasValues() {
        
        return _parameters.containsKey(PARA_VALUES);
    }
}
