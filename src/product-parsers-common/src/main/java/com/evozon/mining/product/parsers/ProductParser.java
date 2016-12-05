package com.evozon.mining.product.parsers;

import org.apache.nutch.storage.WebPage;

public interface ProductParser {
	/**
	 * Callback method for the parsing process: this method gets passed the url and the WebPage
	 * currently being parsed by the {@link org.apache.nutch.parse.ParseFilter} plugin
	 *
	 * @param url  the url currently being parsed
	 * @param page the page currently being parsed
	 */
	boolean parse(String url, WebPage page);
}
