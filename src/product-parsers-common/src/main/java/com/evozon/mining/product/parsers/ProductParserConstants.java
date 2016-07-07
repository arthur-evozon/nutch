package com.evozon.mining.product.parsers;

import org.apache.avro.util.Utf8;

public interface ProductParserConstants {
	static final String PRODUCT_NAME = "product-name";
	static final String PRODUCT_PRICE = "product-price";
	static final String PRODUCT_CURRENCY = "product-currency";
	static final String PRODUCT_DETAILS = "product-meta";

	static final Utf8 META_KEY_PRODUCT_NAME = new Utf8(ProductParserConstants.PRODUCT_NAME);
	static final Utf8 META_KEY_PRODUCT_PRICE = new Utf8(ProductParserConstants.PRODUCT_PRICE);
	static final Utf8 META_KEY_PRODUCT_CURRENCY = new Utf8(ProductParserConstants.PRODUCT_CURRENCY);
	static final Utf8 META_KEY_PRODUCT_DETAILS = new Utf8(ProductParserConstants.PRODUCT_DETAILS);

}
