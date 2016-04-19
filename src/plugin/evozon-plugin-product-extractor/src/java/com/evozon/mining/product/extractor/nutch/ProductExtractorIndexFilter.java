package com.evozon.mining.product.extractor.nutch;

import com.evozon.mining.product.parsers.DefaultProductParser;
import com.evozon.mining.product.parsers.ProductParser;
import org.apache.avro.util.Utf8;
import org.apache.hadoop.conf.Configuration;
import org.apache.nutch.indexer.IndexingException;
import org.apache.nutch.indexer.IndexingFilter;
import org.apache.nutch.indexer.NutchDocument;
import org.apache.nutch.storage.WebPage;
import org.apache.nutch.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.HashSet;

/**
 * The indexing portion of the TagExtractor module. Retrieves the
 * productDetail information stuffed into the ParseResult object by the parse
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
		ByteBuffer productDefinition = page.getMetadata().get(new Utf8(ProductParser.PRODUCT_KEY));
		if (productDefinition == null || productDefinition.remaining() == 0) {
			return doc;
		}

		// add to the nutch document, the properties of the field are set in
		// the addIndexBackendOptions method.
		String productName = new String(productDefinition.array());

		ByteBuffer productPriceBytes = page.getMetadata().get(new Utf8(ProductParser.PRODUCT_PRICE));
		if (productPriceBytes == null || productPriceBytes.remaining() == 0) {
			return doc;
		}

		double productPrice = DefaultProductParser.toDouble(productPriceBytes.array());
		String priceStr = String.format("%.2f", productPrice);

		ByteBuffer productCurrencyBytes = page.getMetadata().get(new Utf8(ProductParser.PRODUCT_CURRENCY));
		if (productCurrencyBytes == null || productCurrencyBytes.remaining() == 0) {
			return doc;
		}

		String productCurrency = new String(productCurrencyBytes.array());

		doc.add(ProductParser.PRODUCT_KEY, productName);
		doc.add(ProductParser.PRODUCT_PRICE, priceStr);
		doc.add(ProductParser.PRODUCT_CURRENCY, productCurrency);

		LOG.info("\n\t>>>> Adding product: [ {} : {} ] for URL: {}", productName, priceStr, url.toString());

		ByteBuffer productDetailsBuffer = page.getMetadata().get(new Utf8(ProductParser.PRODUCT_DETAILS));
		if (productDetailsBuffer == null || productDetailsBuffer.remaining() == 0) {
			return doc;
		}

		// add to the nutch document, the properties of the field are set in
		// the addIndexBackendOptions method.

		String[] productDetails = Bytes.toString(productDetailsBuffer).split("\n");
		for (String productDetail : productDetails) {
			productDetail = productDetail.replaceAll("&gt;", "").trim();
			LOG.trace("+ [ {} ]", productDetail);
			doc.add(ProductParser.PRODUCT_DETAILS, productDetail);
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