/*
* Licensed to the Apache Software Foundation (ASF) under one or more
* contributor license agreements.  See the NOTICE file distributed with
* this work for additional information regarding copyright ownership.
* The ASF licenses this file to You under the Apache License, Version 2.0
* (the "License"); you may not use this file except in compliance with
* the License.  You may obtain a copy of the License at
*
*    http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing, software
* distributed under the License is distributed on an "AS IS" BASIS,
* WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
* See the License for the specific language governing permissions and
* limitations under the License.
*/

package water;

import java.util.*;

public class SparseVector implements Vector {
    private int[] _indices;
    private double[] _values;
    private int _size;
    private int _nnz;

    @Override
    public int size() {
        return _size;
    }

    public int nnz() {
        return _nnz;
    }

    public SparseVector(double[] v) {
        this(new DenseVector(v));
    }

    public SparseVector(Map<Integer, Double> mapping) {
        this._size = mapping.size();
        this._nnz = this._size;

        List<Map.Entry<Integer, Double>> entryList = new ArrayList<>(mapping.entrySet());

        Collections.sort(entryList, new Comparator<Map.Entry<Integer, Double>>() {
            @Override
            public int compare(Map.Entry<Integer, Double> o1, Map.Entry<Integer, Double> o2) {
                return o1.getKey().compareTo(o2.getKey());
            }
        });

        int i = 0;
        this._indices = new int[this._size];
        this._values = new double[this._size];

        for (Map.Entry<Integer, Double> entry : entryList) {
            this._indices[i] = entry.getKey();
            this._values[i] = entry.getValue();
            i++;
        }
    }

    public SparseVector(int size, int[] i, double[] v) {
        this._size = size;
        this._nnz = size;
        this._indices = i;
        this._values = v;
    }

    public SparseVector(final DenseVector dv) {
        _size = dv.size();
        // first count non-zeros
        for (int i = 0; i < dv.raw().length; ++i) {
            if (dv.get(i) != 0.0) {
                _nnz++;
            }
        }
        // only allocate what's needed
        _indices = new int[_nnz];
        _values = new double[_nnz];
        // fill values
        int idx = 0;
        for (int i = 0; i < dv.raw().length; ++i) {
            if (dv.get(i) != 0.0f) {
                _indices[idx] = i;
                _values[idx] = dv.get(i);
                idx++;
            }
        }
        assert (idx == nnz());
    }

    /**
     * Slow path access to i-th element
     *
     * @param i element index
     * @return real value
     */
    @Override
    public double get(int i) {
        final int idx = Arrays.binarySearch(_indices, i);
        return idx < 0 ? 0f : _values[idx];
    }

    @Override
    public void set(int i, double val) {
        final int idx = Arrays.binarySearch(_indices, i);
        if (idx >= 0) {
            _values[idx] = val;
        }
    }

    @Override
    public void add(int i, double val) {
        final int idx = Arrays.binarySearch(_indices, i);
        if (idx >= 0) {
            _values[idx] += val;
        }
    }

    @Override
    public double[] raw() {
        throw new UnsupportedOperationException("Raw access to the data in a sparse vector is not implemented.");
    }

    @Override
    public int[] indices() {
        // Should use clone()? I'm worried about the memory footprint
        return this._indices;
    }

    @Override
    public boolean isSparse() {
        return true;
    }

    @Override
    public Vector clone() {
        return new SparseVector(this.size(), this._indices.clone(), this._values.clone());
    }

    @Override
    public String toString() {
        return "(" + _size + "," + Arrays.toString(_indices) + "," + Arrays.toString(_values) + ")";
    }

}
