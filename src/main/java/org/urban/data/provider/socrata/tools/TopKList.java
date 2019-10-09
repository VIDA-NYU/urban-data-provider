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
package org.urban.data.provider.socrata.tools;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Iterator;
import org.urban.data.provider.socrata.db.Dataset;

/**
 *
 * @author Heiko Mueller <heiko.mueller@nyu.edu>
 */
public class TopKList implements Iterable<TopKListEntry> {
    
    private final TopKListEntry[] _elements;
    private int _size;
    
    public TopKList(int k) {

        _elements = new TopKListEntry[k];
        _size = 0;
    }
    
    public synchronized void add(Dataset dataset, String column, BigDecimal sim) {

        TopKListEntry entry = new TopKListEntry(dataset, column, sim);
        if (_size == 0) {
            _elements[0] = entry;
            _size++;
        } else if (_size < _elements.length) {
            this.insert(entry);
            _size++;
        } else if (_elements[_elements.length - 1].sim().compareTo(sim) < 0) {
            this.insert(entry);
        }
    }

    public TopKListEntry get(int index) {
        
        return _elements[index];
    }
    
    private void insert(TopKListEntry entry) {

        int lower = 0;
        int upper = _size - 1;
        int mid = -1;
        boolean inserted = false;
        while (lower <= upper) {
            mid = (upper + lower) / 2;
            int comp = _elements[mid].compareTo(entry);
            if (comp < 0) {
                upper = mid - 1;
            } else if (comp > 0) {
                lower = mid + 1;
            } else {
                this.insert(mid, entry);
                inserted = true;
                break;
            }
        }
        if (!inserted) {
            if (_elements[mid].compareTo(entry) < 0) {
                this.insert(mid, entry);
            } else {
                this.insert(mid + 1, entry);
            }
        }
    }
    
    private void insert(int pos, TopKListEntry entry) {
        
        if (_size < _elements.length) {
            _elements[_size] = _elements[_size - 1];
        }
        for (int iEntry = _size - 1; iEntry > pos; iEntry--) {
            _elements[iEntry] = _elements[iEntry - 1];
        }
        _elements[pos] = entry;
    }

    @Override
    public Iterator<TopKListEntry> iterator() {

        ArrayList<TopKListEntry> elements = new ArrayList<>();
	for (TopKListEntry entry : _elements) {
	    if (entry != null) {
                elements.add(entry);
            }
        }
        return elements.iterator();
    }
    
    public int size() {
        
        return _size;
    }
}
