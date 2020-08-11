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

import java.util.HashMap;

/**
 * Abstract base class for commands that handles descriptions, parameters, and
 * the command name.
 * 
 * @author Heiko Mueller <heiko.mueller@nyu.edu>
 */
public abstract class CommandImpl implements Command {

    private final String _longDescription;
    private final String _name;
    private final HashMap<String, String> _parameters;
    private final String _shortDescription;
    
    public CommandImpl(String name, String shortDescription, String longDescription) {
        
        _name = name;
        _shortDescription = shortDescription;
        _longDescription = longDescription;
        
        _parameters = new HashMap<>();
        _parameters.put(Args.PARA_BASEDIR, "Base directory for the archive");
    }
    
    public CommandImpl(String name, String shortDescription) {
    
        this(name, shortDescription, null);
    }
    
    public final void addParameter(String name, String text) {
        
        if (text == null) {
            if (name.equals(Args.PARA_COLUMN)) {
                _parameters.put(name, "Column name or index");
            } else if (name.equals(Args.PARA_DATE)) {
                _parameters.put(name, "Download date");
            } else if (name.equals(Args.PARA_DATASET)) {
                _parameters.put(name, "Unique dataset identifier");
            } else if (name.equals(Args.PARA_DOMAIN)) {
                _parameters.put(name, "Unique domain name");
            } else if (name.equals(Args.PARA_EXISTING)) {
                _parameters.put(name, "Include downloaded datasets only");
            } else if (name.equals(Args.PARA_HTML)) {
                _parameters.put(name, "Delete (potential) HTML files");
            } else if (name.equals(Args.PARA_ORDERBY)) {
                _parameters.put(name, "Order by value of count");
            } else if (name.equals(Args.PARA_OUTPUT)) {
                _parameters.put(name, "Output file");
            } else if (name.equals(Args.PARA_OVERWRITE)) {
                _parameters.put(name, "Overwrite existing file");
            } else if (name.equals(Args.PARA_REPORT)) {
                _parameters.put(name, "Print actions but do not execute");
            } else if (name.equals(Args.PARA_REVERSE)) {
                _parameters.put(name, "Reverse default output order");
            } else if (name.equals(Args.PARA_STATS)) {
                _parameters.put(name, "Only output statistics");
            } else if (name.equals(Args.PARA_THREADS)) {
                _parameters.put(name, "Number of parallel threads used");
            } else if (name.equals(Args.PARA_VALUES)) {
                _parameters.put(name, "List of values");
            } else{
                _parameters.put(name, "");
            }
        } else {
            _parameters.put(name, text);
        }
    }
    
    public final void addParameter(String name) {
    
        this.addParameter(name, null);
    }
    
    @Override
    public String longDescription() {

        return _longDescription;
    }

    @Override
    public String name() {

        return _name;
    }

    @Override
    public HashMap<String, String> parameters() {

        return _parameters;
    }

    @Override
    public String shortDescription() {

        return _shortDescription;
    }
}
