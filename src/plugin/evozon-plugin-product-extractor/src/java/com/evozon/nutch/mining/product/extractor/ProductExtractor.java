package com.evozon.nutch.mining.product.extractor;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.nutch.indexer.IndexingException;
import org.apache.nutch.indexer.IndexingFilter;
import org.apache.nutch.indexer.NutchDocument;
import org.apache.nutch.storage.WebPage;

import java.util.Collection;
import java.util.HashSet;

public class ProductExtractor implements IndexingFilter {

	private static final Collection<WebPage.Field> FIELDS = new HashSet<WebPage.Field>();
	static {
		FIELDS.add(WebPage.Field.TITLE);
		FIELDS.add(WebPage.Field.TEXT);
	}

	private static final Log LOG = LogFactory.getLog(ProductExtractor.class);
	private Configuration conf;

	//implements the filter-method which gives you access to important Objects like NutchDocument
	public NutchDocument filter(NutchDocument doc, String url, WebPage page) throws IndexingException {

		String content = page.toString();
		//adds the new field to the document
		doc.add("pageLength", String.valueOf(content.length()));
		return doc;
	}

	//Boilerplate
	public Configuration getConf() {
		return conf;
	}

	//Boilerplate
	public void setConf(Configuration conf) {
		this.conf = conf;
	}

	@Override
	public Collection<WebPage.Field> getFields() {
		return FIELDS;
	}
}