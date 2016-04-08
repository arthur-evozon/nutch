package com.evozon.mining.product.extractor.nutch;

import org.apache.nutch.storage.WebPage;

public interface ProductParser {
	static final String PRODUCT_KEY = "product-name";
	static final String PRODUCT_PRICE = "product-price";
	static final String PRODUCT_CURRENCY = "product-currency";
	static final String PRODUCT_DETAILS = "product-meta";

	static final String NAME = "product-name";
	static final String PRICE_WHOLE = "product-price/whole";
	static final String PRICE_PART = "product-price/part";
	static final String PRICE_CURRENCY = "product-price/currency";
	static final String META = "product-meta";

	/**
	 * Initialization method for the parser: here the parser can load its configuration up
	 */
	void initializeParser();

	/**
	 * Callback method for the parsing process: this method gets passed the url and the WebPage
	 * currently being parsed by the {@link org.apache.nutch.parse.ParseFilter} plugin
	 *
	 * @param url  the url currently being parsed
	 * @param page the page currently being parsed
	 */
	void parse(String url, WebPage page);
}
