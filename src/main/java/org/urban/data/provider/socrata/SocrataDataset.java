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

import com.google.gson.Gson;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.google.gson.stream.JsonReader;
import com.google.gson.stream.JsonWriter;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.net.URL;
import java.util.ArrayList;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.commons.io.IOUtils;
import org.apache.http.client.utils.URIBuilder;
import org.urban.data.core.util.FileSystem;

/**
 *
 * @author Heiko Mueller <heiko.mueller@nyu.edu>
 */
public class SocrataDataset {
    
    public static final int DEFAULT_MAXATTEMPTS = 3;
    public static final int LIMIT = 50000;
    
    private String _identifier;
    private URL _url = null;
    
    public SocrataDataset(String resourceUrl, boolean retrieveTrueResource) throws java.io.IOException, java.net.MalformedURLException {
        
        _identifier = resourceUrl.substring(resourceUrl.lastIndexOf("/") + 1);
        if (_identifier.contains(".")) {
            _identifier = _identifier.substring(0, _identifier.indexOf("."));
        }
        
        if (retrieveTrueResource) {
            try (BufferedReader in = new BufferedReader(
                    new InputStreamReader(new URL(resourceUrl).openStream(), "UTF-8")
            )) {
                String line;
                while ((line = in.readLine()) != null) {
                    int pos = line.indexOf("\"resourceUrl\"");
                    if (pos != -1) {
                        line = line.substring(pos + 13);
                        pos = line.indexOf("\"");
                        line = line.substring(pos + 1, line.indexOf("\"", pos + 1));
                        _url = new URL(line);
                        break;
                    }
                    while ((pos = line.indexOf(" value=\"")) != -1) {
                        line = line.substring(pos + 8);
                        pos = line.indexOf("\"");
                        String url = line.substring(0, pos);
                        if (url.endsWith(".json")) {
                            _url = new URL(line);
                            break;
                        }
                        line = line.substring(pos + 1);
                    }
                }
            }
            if (_url == null) {
                throw new IllegalArgumentException("No resource Url found");
            }
        } else {
            _url = new URL(resourceUrl);
        }
    }
    
    public SocrataDataset(String resourceUrl) throws java.io.IOException, java.net.MalformedURLException {
        
        this(resourceUrl, true);
    }

    private void concat(Iterable<File> files, OutputStream os) throws java.io.IOException {
        
        try (JsonWriter writer = new JsonWriter(new OutputStreamWriter(os, "UTF-8"))) {
            writer.beginArray();
            Gson gson = new Gson();
            for (File file : files) {
                try (JsonReader reader = new JsonReader(
                        new InputStreamReader(new FileInputStream(file), "UTF-8")
                )) {
                    reader.beginArray();
                    while (reader.hasNext()) {
                        JsonObject object = new JsonParser().parse(reader).getAsJsonObject();
                        gson.toJson(object, writer);
                    }
                    reader.endArray();
                }
            }
            writer.endArray();
        }
    }
    
    public void download(File directory, boolean overwrite, int maxAttempts) throws java.net.URISyntaxException, java.io.IOException {
	
        String entryUrl = _url.toExternalForm();
	String filename = _identifier + ".json.gz";
	File dataFile = new File(
            directory.getAbsolutePath() + File.separator + filename
        );
	if ((dataFile.exists()) && (!overwrite)) {
	    return;
	}
	
	ArrayList<File> files = new ArrayList<>();
	boolean done = false;
	int offset = 0;
	int attempt = 0;
	
        int posScheme = entryUrl.indexOf("://");
        String protocol = entryUrl.substring(0, posScheme);
        entryUrl = entryUrl.substring(posScheme + 3);
        int posPath = entryUrl.indexOf("/");
        String host, path;
        if (posPath == -1) {
            host = entryUrl;
            path = "";
        } else {
            host = entryUrl.substring(0, posPath);
            path = entryUrl.substring(posPath + 1);
        }
        
        while (!done) {
	    File file = new File(
                directory.getAbsolutePath() + File.separator + filename + "." + files.size()
            );
	    if ((!file.exists()) || (overwrite)) {
                URIBuilder uri = new URIBuilder()
                    .setScheme(protocol)
                    .setHost(host)
                    .setPath(path)
                    .setParameter("$offset", Integer.toString(offset))
                    .setParameter("$limit", Integer.toString(LIMIT));
		System.out.println(uri.toString());
		try (
                    InputStream is = new URL(uri.toString()).openStream();
                    OutputStream os = new FileOutputStream(file)
                ) {
		    IOUtils.copy(is, os);
		} catch (java.io.IOException ex) {
		    file.delete();
                    Logger.getLogger(
                        this.getClass().getPackage().getName()
                    ).log(Level.SEVERE, null, ex);
		}
	    }
	    
	    /*
	     * Test if downloaded file is correct. Otherwise retry download.
	     */
	    int objectCount = this.parseFile(file);
	    switch (objectCount) {
	    	case -1:
		    attempt++;
		    if (!file.delete()) {
			attempt = maxAttempts;
		    }
		    if (attempt >= maxAttempts) {
			for (File downloadedFile : files) {
			    downloadedFile.delete();
			}
                        String msg = "Max. number of attempts for file " + file.getName() + " (" + _url.toExternalForm() + ") reached";
                        Logger.getLogger(
                            this.getClass().getPackage().getName()
                        ).log(Level.SEVERE, msg);
			throw new java.io.IOException(msg);
		    }
		    break;
	    	case 0:
		    done = true;
		    file.delete();
		    break;
	    	default:
		    files.add(file);
		    done = (objectCount < LIMIT);
		    offset += objectCount;
		    attempt = 0;
		    break;
	    }
	}
	
        // Create final output file. May require to concatenate multiple
        // downloaded files.
        this.concat(files, FileSystem.openOutputFile(dataFile));
        for (File file : files) {
            file.delete();
        }
    }

    public void download(File directory, boolean overwrite) throws java.net.URISyntaxException, java.io.IOException {
        
        this.download(directory, overwrite, DEFAULT_MAXATTEMPTS);
    }

    public void download(File directory) throws java.net.URISyntaxException, java.io.IOException {
        
        this.download(directory, true, DEFAULT_MAXATTEMPTS);
    }

    public String identifier() {
        
        return _identifier;
    }
    
    /**
     * Parses the given JSON file to ensure that it's content is valid. Returns
     * the total number of objects. The result is -1 if an error occured during
     * parsing.
     * 
     * @param file
     * @return
     * @throws java.io.IOException 
     */
    private int parseFile(File file) throws java.io.IOException {
	
	if (!file.exists()) {
            String msg = file.getAbsolutePath() + " does not exist";
            Logger.getLogger(
                this.getClass().getPackage().getName()
            ).log(Level.SEVERE, msg);
	    return -1;
	}
	
	int result = 0;
	
	try (JsonReader reader = new JsonReader(new InputStreamReader(new FileInputStream(file), "UTF-8"))) {
	    reader.beginArray();
	    while (reader.hasNext()) {
		new JsonParser().parse(reader).getAsJsonObject();
		result++;
	    }
	    reader.endArray();
	} catch (Exception ex) {
            Logger.getLogger(
                this.getClass().getPackage().getName()
            ).log(Level.SEVERE, null, ex);
	    result = -1;
	}

	return result;	
    }

    public URL url() {
        
        return _url;
    }
    
    public static void main(String[] args) {
        
        if (args.length != 2) {
            System.out.println("Usage: <url> <directory>");
            System.exit(-1);
        }
        
        String url = args[0];
        File directory = new File(args[1]);
        
        try {
            new SocrataDataset(url, true).download(directory, true, 1);
        } catch (java.net.URISyntaxException | java.io.IOException ex) {
            Logger.getLogger(SocrataDataset.class.getName()).log(Level.SEVERE, url, ex);
            System.exit(-1);
        }
    }
}
