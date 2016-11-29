package com.evozon.mining.product.parsers.emag;

import com.evozon.mining.product.parsers.DefaultProductParser;
import com.evozon.mining.product.parsers.ProductParser;
import com.evozon.mining.product.parsers.ProductParserUtils;
import org.apache.nutch.storage.WebPage;
import org.jsoup.nodes.Document;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class EmagProductParser extends DefaultProductParser implements ProductParser {
	private static final Logger LOG = LoggerFactory.getLogger(EmagProductParser.class);

	static final String[] DETAILS_NESTED_SELECT_PATTERN = new String[]{"tbody", "tr", "td"};

	private final String[] detailsNestedSelectPattern;

	public EmagProductParser() {
		this(DETAILS_NESTED_SELECT_PATTERN);
	}

	public EmagProductParser(String... detailsNestedSelectPattern) {
		super();
		this.detailsNestedSelectPattern = detailsNestedSelectPattern;
	}

	@Override
	protected String parseProductMeta(String url, WebPage page, Document document, String metaSelectors) {
		return ProductParserUtils.buildNameValuesString(ProductParserUtils.getAllNameValuesWithSelectorPattern(document, metaSelectors,
				detailsNestedSelectPattern));
	}
}