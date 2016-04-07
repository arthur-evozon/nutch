package com.evozon.mining.product.parsers;

import com.evozon.mining.product.extractor.nutch.ProductParser;
import org.apache.nutch.storage.WebPage;
import org.jsoup.nodes.Document;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class IntendProductParser extends DefaultProductParser implements ProductParser {

	private static final Logger LOG = LoggerFactory.getLogger(IntendProductParser.class);

	private static final String PRODUCT_MARKER = "Prezentare produs:";

	@Override
	String parseProductName(String url, WebPage page, Document document, String selector) {
		String productName = super.parseProductName(url, page, document, selector);

		if (!productName.startsWith(PRODUCT_MARKER)) {
			return null;
		}

		return productName.substring(productName.indexOf(PRODUCT_MARKER) + PRODUCT_MARKER.length()).trim();
	}

	@Override
	String parseProductPriceCurrency(String url, WebPage page, Document document, String selector) {
		return extractText(document, selector, 3).trim();
	}
}