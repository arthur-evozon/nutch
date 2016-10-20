/*******************************************************************************
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 ******************************************************************************/
package org.apache.nutch.crawl;

import org.apache.avro.util.Utf8;
import org.apache.gora.mapreduce.GoraReducer;
import org.apache.hadoop.conf.Configuration;
import org.apache.nutch.crawl.GeneratorJob.SelectorEntry;
import org.apache.nutch.fetcher.FetcherJob.FetcherMapper;
import org.apache.nutch.storage.Mark;
import org.apache.nutch.storage.WebPage;
import org.apache.nutch.util.TableUtil;
import org.apache.nutch.util.URLUtil;

import java.io.IOException;
import java.net.MalformedURLException;
import java.util.HashMap;
import java.util.Map;

import static org.apache.nutch.crawl.GeneratorJob.COUNTER_GROUP_GENERATE;

/**
 * Reduce class for generate
 * <p>
 * The #reduce() method write a random integer to all generated URLs. This
 * random number is then used by {@link FetcherMapper}.
 */
public class GeneratorReducer extends GoraReducer<SelectorEntry, WebPage, String, WebPage> {
	static final String COUNTER_GENERATE_MARK = "GENERATE_MARK";
	static final String COUNTER_MALFORMED_URL = "MALFORMED_URL";

	private long count;
	private long limit;
	private long maxCount;
	private boolean byDomain = false;
	private Map<String, Integer> hostCountMap = new HashMap<String, Integer>();
	private Utf8 batchId;

	@Override
	protected void reduce(SelectorEntry key, Iterable<WebPage> values, Context context) throws IOException, InterruptedException {
		for (WebPage page : values) {
			if (count >= limit) {
				return;
			}
			if (maxCount > 0) {
				String hostordomain;
				if (byDomain) {
					hostordomain = URLUtil.getDomainName(key.url);
				} else {
					hostordomain = URLUtil.getHost(key.url);
				}

				Integer hostCount = hostCountMap.get(hostordomain);
				if (hostCount == null) {
					hostCountMap.put(hostordomain, 0);
					hostCount = 0;
				}
				if (hostCount >= maxCount) {
					return;
				}
				hostCountMap.put(hostordomain, hostCount + 1);
			}

			Mark.GENERATE_MARK.putMark(page, batchId);
			page.setBatchId(batchId);
			try {
				context.write(TableUtil.reverseUrl(key.url), page);
			} catch (MalformedURLException e) {
				context.getCounter(COUNTER_GROUP_GENERATE, COUNTER_MALFORMED_URL).increment(1);
				continue;
			}
			count++;
			context.getCounter(COUNTER_GROUP_GENERATE, COUNTER_GENERATE_MARK).increment(1);
		}
	}

	@Override
	protected void setup(Context context) throws IOException, InterruptedException {
		Configuration conf = context.getConfiguration();
		long totalLimit = conf.getLong(GeneratorJob.GENERATOR_TOP_N, Long.MAX_VALUE);
		if (totalLimit == Long.MAX_VALUE) {
			limit = Long.MAX_VALUE;
		} else {
			limit = totalLimit / context.getNumReduceTasks();
		}
		maxCount = conf.getLong(GeneratorJob.GENERATOR_MAX_COUNT, -2);
		batchId = new Utf8(conf.get(GeneratorJob.BATCH_ID));
		String countMode = conf.get(GeneratorJob.GENERATOR_COUNT_MODE, GeneratorJob.GENERATOR_COUNT_VALUE_HOST);
		if (countMode.equals(GeneratorJob.GENERATOR_COUNT_VALUE_DOMAIN)) {
			byDomain = true;
		}

	}

}