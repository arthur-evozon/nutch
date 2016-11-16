package com.evozon.mining.product.indexwriter.elastic;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.nutch.indexer.IndexWriter;
import org.apache.nutch.indexer.NutchDocument;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.ListenableActionFuture;
import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.delete.DeleteRequestBuilder;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.node.Node;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import static org.elasticsearch.node.NodeBuilder.nodeBuilder;

/**
 * An almost identical copy of the ElasticIndexWriter class (same functionality):
 * the idea behind this design is to allow parsed products to be indexed in any way we like (depending on the product model)
 */
public abstract class DefaultElasticIndexWriter implements IndexWriter {
	public static Logger LOG = LoggerFactory.getLogger(DefaultElasticIndexWriter.class);

	private static final int DEFAULT_MAX_BULK_DOCS = 250;
	private static final int DEFAULT_MAX_BULK_LENGTH = 2500500;

	Client client;
	private Node node;
	String defaultIndex;

	private Configuration config;

	BulkRequestBuilder bulk;
	private ListenableActionFuture<BulkResponse> execute;
	private int port = -1;
	private String host = null;
	private String clusterName = null;
	int maxBulkDocs;
	int maxBulkLength;
	long indexedDocs = 0;
	int bulkDocs = 0;
	int bulkLength = 0;
	boolean createNewBulk = false;

	@Override
	public void open(Configuration job) throws IOException {
		clusterName = job.get(ElasticConstants.CLUSTER);
		host = job.get(ElasticConstants.HOST);
		port = job.getInt(ElasticConstants.PORT, 9300);

		ImmutableSettings.Builder settingsBuilder = ImmutableSettings.settingsBuilder().classLoader(Settings.class.getClassLoader());

		BufferedReader reader = new BufferedReader(job.getConfResourceAsReader("elasticsearch.conf"));
		String line;
		String parts[];

		while ((line = reader.readLine()) != null) {
			if (StringUtils.isNotBlank(line) && !line.startsWith("#")) {
				line.trim();
				parts = line.split("=");

				if (parts.length == 2) {
					settingsBuilder.put(parts[0].trim(), parts[1].trim());
				}
			}
		}

		if (StringUtils.isNotBlank(clusterName)) settingsBuilder.put("cluster.name", clusterName);

		// Set the cluster name and build the settings
		Settings settings = settingsBuilder.build();

		// Prefer TransportClient
		if (host != null && port > 1) {
			client = new TransportClient(settings).addTransportAddress(new InetSocketTransportAddress(host, port));
		} else if (clusterName != null) {
			node = nodeBuilder().settings(settings).client(true).node();
			client = node.client();
		}

		bulk = client.prepareBulk();
		defaultIndex = job.get(ElasticConstants.INDEX, "nutch");
		maxBulkDocs = job.getInt(ElasticConstants.MAX_BULK_DOCS, DEFAULT_MAX_BULK_DOCS);
		maxBulkLength = job.getInt(ElasticConstants.MAX_BULK_LENGTH, DEFAULT_MAX_BULK_LENGTH);
	}

	@Override
	public void write(NutchDocument doc) throws IOException {
		String id = doc.getFieldValue("id");
		String type = doc.getDocumentMeta().get("type");
		if (type == null) {
			type = "doc";
		}

		IndexRequestBuilder request = client.prepareIndex(defaultIndex, type, id);
		Map<String, Object> source = new HashMap<String, Object>();

		// Loop through all fields of this doc
		for (String fieldName : doc.getFieldNames()) {
			processIndexedField(doc, source, fieldName);
		}

		request.setSource(source);

		// Add this indexing request to a bulk request
		bulk.add(request);
		indexedDocs++;
		bulkDocs++;

		if (bulkDocs >= maxBulkDocs || bulkLength >= maxBulkLength) {
			LOG.info("Processing bulk request [docs = " + bulkDocs + ", length = " + bulkLength + ", total docs = " + indexedDocs + ", " +
					"last doc in bulk = '" + id + "']");
			// Flush the bulk of indexing requests
			createNewBulk = true;
			commit();
		}

	}

	Map<String, Object> processIndexedField(NutchDocument doc, Map<String, Object> source, String fieldName) {
		if (doc.getFieldValues(fieldName).size() > 1) {
			source.put(fieldName, doc.getFieldValue(fieldName));
			// Loop through the values to keep track of the size of this document
			for (Object value : doc.getFieldValues(fieldName)) {
				bulkLength += value.toString().length();
			}
		} else {
			source.put(fieldName, doc.getFieldValue(fieldName));
			bulkLength += doc.getFieldValue(fieldName).toString().length();
		}

		return source;
	}

	@Override
	public void delete(String key) throws IOException {
		try {
			DeleteRequestBuilder builder = client.prepareDelete();
			builder.setIndex(defaultIndex);
			builder.setType("doc");
			builder.setId(key);
			builder.execute().actionGet();
		} catch (ElasticsearchException e) {
			throw makeIOException(e);
		}
	}

	public static IOException makeIOException(ElasticsearchException e) {
		final IOException ioe = new IOException();
		ioe.initCause(e);
		return ioe;
	}

	@Override
	public void update(NutchDocument doc) throws IOException {
		write(doc);
	}

	@Override
	public void commit() throws IOException {
		if (execute != null) {
			// wait for previous to finish
			long beforeWait = System.currentTimeMillis();
			BulkResponse actionGet = execute.actionGet();
			if (actionGet.hasFailures()) {
				for (BulkItemResponse item : actionGet) {
					if (item.isFailed()) {
						throw new RuntimeException("First failure in bulk: " + item.getFailureMessage());
					}
				}
			}
			long msWaited = System.currentTimeMillis() - beforeWait;
			LOG.info("Previous took in ms " + actionGet.getTookInMillis() + ", including wait " + msWaited);
			execute = null;
		}
		if (bulk != null) {
			if (bulkDocs > 0) {
				// start a flush, note that this is an asynchronous call
				execute = bulk.execute();
			}
			bulk = null;
		}
		if (createNewBulk) {
			// Prepare a new bulk request
			bulk = client.prepareBulk();
			bulkDocs = 0;
			bulkLength = 0;
		}
	}

	@Override
	public void close() throws IOException {
		// Flush pending requests
		LOG.info("Processing remaining requests [docs = " + bulkDocs + ", length = " + bulkLength + ", total docs = " + indexedDocs + "]");
		createNewBulk = false;
		commit();
		// flush one more time to finalize the last bulk
		LOG.info("Processing to finalize last execute");
		createNewBulk = false;
		commit();

		// Close
		client.close();
		if (node != null) {
			node.close();
		}
	}

	@Override
	public String describe() {
		StringBuffer sb = new StringBuffer("ProductsElasticIndexWriter\n");
		sb.append("\t").append(ElasticConstants.CLUSTER).append(" : elastic prefix cluster\n");
		sb.append("\t").append(ElasticConstants.HOST).append(" : hostname\n");
		sb.append("\t").append(ElasticConstants.PORT).append(" : port  (default 9300)\n");
		sb.append("\t").append(ElasticConstants.INDEX).append(" : elastic index command \n");
		sb.append("\t").append(ElasticConstants.MAX_BULK_DOCS).append(" : elastic bulk index doc counts. (default 250) \n");
		sb.append("\t").append(ElasticConstants.MAX_BULK_LENGTH).append(" : elastic bulk index length. (default 2500500 ~2.5MB)\n");
		return sb.toString();
	}

	@Override
	public void setConf(Configuration conf) {
		config = conf;
		String cluster = conf.get(ElasticConstants.CLUSTER);
		String host = conf.get(ElasticConstants.HOST);

		if (StringUtils.isBlank(cluster) && StringUtils.isBlank(host)) {
			String message = "Missing elastic.cluster and elastic.host. At least one of them should be set in nutch-site.xml ";
			message += "\n" + describe();
			LOG.error(message);
			throw new RuntimeException(message);
		}
	}

	@Override
	public Configuration getConf() {
		return config;
	}
}
