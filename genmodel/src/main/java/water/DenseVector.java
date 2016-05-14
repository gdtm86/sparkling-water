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

import java.util.Arrays;

public class DenseVector implements Vector {
    private double[] _data;
    public DenseVector(int len) { _data = new double[len]; }
    public DenseVector(double[] v) { _data = v.clone(); }
    @Override public double get(int i) { return _data[i]; }
    @Override public void set(int i, double val) { _data[i] = val; }
    @Override public void add(int i, double val) { _data[i] += val; }
    @Override public int size() { return _data.length; }
    @Override public double[] raw() { return _data.clone(); }

    @Override
    public int[] indices() {
        // Well could be but not really necessary
        throw new UnsupportedOperationException("Indices access in a dense vector is not supported.");
    }

    @Override
    public boolean isSparse() { return false; }

    @Override
    public Vector clone() {
        return new DenseVector(this._data);
    }

    @Override public String toString() { return Arrays.toString(_data); }
}