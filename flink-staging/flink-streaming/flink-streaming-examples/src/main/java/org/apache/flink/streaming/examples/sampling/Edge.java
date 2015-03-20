/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.streaming.examples.sampling;

import org.apache.flink.api.java.tuple.Tuple2;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;

/**
 * Created by marthavk on 2015-03-11.
 */
public class Edge<VV extends Serializable> extends Tuple2<VV, VV> {

	public Edge() {
	}

	public Edge(VV value0, VV value1) {
		super(value0, value1);
	}

	public VV getSrc() {
		return this.f0;
	}

	public VV getTrg() {
		return this.f1;
	}

	public boolean isAdjacentTo(Edge e) {
		HashSet h = new HashSet();
		h.add(this.getSrc());
		h.add(this.getTrg());
		if (h.contains(e.getSrc()) || h.contains(e.getTrg())) {
			return true;
		}
		return false;
	}

	public ArrayList<VV> getVertices() {
		ArrayList<VV> vertices = new ArrayList<VV>();
		vertices.add(this.getSrc());
		vertices.add(this.getTrg());
		return vertices;
	}


}

