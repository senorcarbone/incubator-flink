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

package org.apache.flink.streaming.util.extractor;

import org.apache.flink.streaming.api.invokable.operator.NextGenExtractor;

public class ConcatinatedExtract<FROM, OVER, TO> implements NextGenExtractor<FROM, TO> {

	/**
	 * auto-generated id
	 */
	private static final long serialVersionUID = -7807197760725651752L;

	private NextGenExtractor<FROM, OVER> e1;
	private NextGenExtractor<OVER, TO> e2;

	public ConcatinatedExtract(NextGenExtractor<FROM, OVER> e1, NextGenExtractor<OVER, TO> e2) {
		this.e1 = e1;
		this.e2 = e2;
	}

	@Override
	public TO extract(FROM in) {
		return e2.extract(e1.extract(in));
	}

	public <OUT> ConcatinatedExtract<FROM, TO, OUT> add(NextGenExtractor<TO, OUT> e3) {
		return new ConcatinatedExtract<FROM, TO, OUT>(this, e3);
	}

}
