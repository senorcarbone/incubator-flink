package org.apache.flink.streaming.examples.sampling;

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

 import java.io.Serializable;
 import java.util.ArrayList;
 import java.util.Random;

/**
 * Created by marthavk on 2015-03-05.
 */
public class Reservoir<T> implements Serializable {

    ArrayList<T> reservoir;
    int maxSize;

    public Reservoir(int size) {
        reservoir = new ArrayList<T>();
        maxSize = size;
        // maxSize = size;
    }

    void insertElement(T e) {
        if (!isFull()) reservoir.add(e);
        else replaceElement(e);
    }


    /**
     * Chooses an existing element uniformly at random and replaces it with e
     * @param e
     */

    void replaceElement(T e) {
        if (isFull()) {
            // choose position uniformly at random
            int pos = new Random().nextInt(maxSize);
            // replace element at pos
            reservoir.set(pos, e);
        }
        else throw new ArrayIndexOutOfBoundsException();
    }

    boolean isFull() {
        return reservoir.isEmpty() ? false :  reservoir.size()==maxSize;
    }


    @Override
    public String toString() {
        return reservoir.toString();
    }


    void print() {
        System.out.println(reservoir.toString());
    }
}
