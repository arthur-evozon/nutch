package com.evozon.mining.product.extractor.nutch;

import org.apache.avro.util.Utf8;
import org.apache.hadoop.conf.Configuration;
import org.apache.nutch.indexer.IndexingException;
import org.apache.nutch.indexer.IndexingFilter;
import org.apache.nutch.indexer.NutchDocument;
import org.apache.nutch.storage.WebPage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.HashSet;

/**
 * The indexing portion of the TagExtractor module. Retrieves the
 * tag information stuffed into the ParseResult object by the parse
 * portion of this module.
 */
public class ProductExtractorIndexFilter implements IndexingFilter {
	private static final Collection<WebPage.Field> FIELDS = new HashSet<WebPage.Field>();

	static {
		FIELDS.add(WebPage.Field.TITLE);
		FIELDS.add(WebPage.Field.TEXT);
	}

	private static final Logger LOG = LoggerFactory.getLogger(ProductExtractorIndexFilter.class);

	private Configuration conf;


	public NutchDocument filter(NutchDocument doc, String url, WebPage page) throws IndexingException {
		ByteBuffer productDefinition = page.getMetadata().get(new Utf8(ProductExtractorParseFilter.PRODUCT_KEY));
		if (productDefinition == null || productDefinition.remaining() == 0) {
			return doc;
		}

		// add to the nutch document, the properties of the field are set in
		// the addIndexBackendOptions method.
		String productName = new String(productDefinition.array());

		ByteBuffer productPriceBytes = page.getMetadata().get(new Utf8(ProductExtractorParseFilter.PRODUCT_PRICE));
		if (productPriceBytes == null || productPriceBytes.remaining() == 0) {
			return doc;
		}

		double productPrice = ProductExtractorParseFilter.toDouble(productPriceBytes.array());
		String priceStr = String.format( "%.2f", productPrice);

		doc.add(ProductExtractorParseFilter.PRODUCT_KEY, productName);
		doc.add(ProductExtractorParseFilter.PRODUCT_PRICE, priceStr);
		LOG.info("\n\t>>>> Adding product: [ {} : {} ] for URL: {}", productName,priceStr, url.toString());

		ByteBuffer productDetails = page.getMetadata().get(new Utf8(ProductExtractorParseFilter.PRODUCT_DETAILS));
		if (productDetails == null || productDetails.remaining() == 0) {
			return doc;
		}

		// add to the nutch document, the properties of the field are set in
		// the addIndexBackendOptions method.
		String productDetailsString = new String(productDetails.array());
		for (String tag : productDetailsString.split("\n")) {
			LOG.trace("Adding tag: [" + tag + "] for URL: " + url.toString());
			doc.add(ProductExtractorParseFilter.PRODUCT_DETAILS, tag);
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
		return null;
	}
}