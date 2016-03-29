package com.evozon.mining.product.extractor.nutch.indexing;

import com.evozon.mining.product.extractor.nutch.parsing.TagExtractorParseFilter;
import org.apache.hadoop.conf.Configuration;
import org.apache.log4j.Logger;
import org.apache.nutch.indexer.IndexingException;
import org.apache.nutch.indexer.IndexingFilter;
import org.apache.nutch.indexer.NutchDocument;
import org.apache.nutch.storage.WebPage;

import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.HashSet;

/**
 * The indexing portion of the TagExtractor module. Retrieves the
 * tag information stuffed into the ParseResult object by the parse
 * portion of this module.
 */
public class TagExtractorIndexFilter implements IndexingFilter {

	private static final Collection<WebPage.Field> FIELDS = new HashSet<WebPage.Field>();

	static {
		FIELDS.add(WebPage.Field.TITLE);
		FIELDS.add(WebPage.Field.TEXT);
	}

	private static final Logger LOGGER = Logger.getLogger(TagExtractorIndexFilter.class);

	private Configuration conf;


	public NutchDocument filter(NutchDocument doc, String url, WebPage page) throws IndexingException {
		ByteBuffer productDefinition = page.getMetadata().get(TagExtractorParseFilter.PRODUCT_KEY);
		if (productDefinition == null || productDefinition.remaining() == 0) {
			return doc;
		}

		// add to the nutch document, the properties of the field are set in
		// the addIndexBackendOptions method.
		String productString = new String(productDefinition.array());
		for (String tag : productString.split("\n")) {
			LOGGER.debug("Adding tag: [" + tag + "] for URL: " + url.toString());
			doc.add(TagExtractorParseFilter.PRODUCT_KEY, tag);
		}

		ByteBuffer productDetails = page.getMetadata().get(TagExtractorParseFilter.PRODUCT_DETAILS);
		if (productDetails == null || productDetails.remaining() == 0) {
			return doc;
		}

		// add to the nutch document, the properties of the field are set in
		// the addIndexBackendOptions method.
		String productDetailsString = new String(productDetails.array());
		for (String tag : productDetailsString.split("\n")) {
			LOGGER.trace("Adding tag: [" + tag + "] for URL: " + url.toString());
			doc.add(TagExtractorParseFilter.PRODUCT_DETAILS, tag);
		}

		return doc;
	}

	public Configuration getConf() {
		return this.conf;
	}

	public void setConf(Configuration conf) {
		this.conf = conf;
	}

	@Override
	public Collection<WebPage.Field> getFields() {
		return FIELDS;
	}
}