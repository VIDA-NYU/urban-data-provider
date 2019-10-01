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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

/**
 * Helper class for default command line arguments of archive management tools.
 * 
 * @author Heiko Mueller <heiko.mueller@nyu.edu>
 */
public class Args {
    
    public final static String PARA_DATASET = "dataset";
    public final static String PARA_DATE = "date";
    public final static String PARA_DOMAIN = "domain";
    
    private final List<String> _args;
    private final HashMap<String, String> _parameters;
    
    public Args(String[] args, String command) {
        
        _args = new ArrayList<>();
        _parameters = new HashMap<>();
        
        for (String arg : args) {
            if (arg.startsWith("--")) {
                String para = arg.substring(2);
                if (!para.contains("=")) {
                    System.out.println(command);
                    System.exit(-1);
                }
                int pos = para.indexOf("=");
                String key = para.substring(0, pos).trim().toLowerCase();
                if ((key.equals(PARA_DATASET)) || (key.equals(PARA_DATE)) || (key.equals(PARA_DOMAIN))) {
                    _parameters.put(
                            key,
                            para.substring(pos + 1).trim()
                    );
                } else {
                    System.out.println(command);
                    System.exit(-1);
                }
            } else {
                _args.add(arg);
            }
        }
    }
    
    public String get(int index) {
    
        return _args.get(index);
    }
    
    public String getDataset() {
        
        return _parameters.get(PARA_DATASET);
    }
    
    public String getDate() {
        
        return _parameters.get(PARA_DATE);
    }
    
    public String getDomain() {
        
        return _parameters.get(PARA_DOMAIN);
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
    
    public int size() {
        
        return _args.size();
    }
}
