package com.evozon.mining.product.extractor.nutch;

import org.apache.nutch.storage.WebPage;

public interface ProductParser {
	public static final String PRODUCT_KEY = "product-name";
	public static final String PRODUCT_PRICE = "product-price";
	public static final String PRODUCT_DETAILS = "product-meta";

	static final String NAME = "name";
	static final String PRICE_WHOLE = "price-whole";
	static final String PRICE_PART = "price-part";
	static final String META = "meta";

	void initializeParser( String host );
	String getHost();

	void parse(String url, WebPage page);
}
