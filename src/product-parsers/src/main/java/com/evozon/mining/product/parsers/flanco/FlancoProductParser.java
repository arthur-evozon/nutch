package com.evozon.mining.product.parsers.flanco;

import com.evozon.mining.product.parsers.DefaultProductParser;
import com.evozon.mining.product.parsers.ProductParser;
import com.evozon.mining.product.parsers.ProductParserUtils;
import org.apache.nutch.storage.WebPage;
import org.jsoup.nodes.Document;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class FlancoProductParser extends DefaultProductParser implements ProductParser {
	private static final Logger LOG = LoggerFactory.getLogger(FlancoProductParser.class);

	static final String META_CONTENT_ATTR = "content";

	private final String metaContentAttr;

	public FlancoProductParser() {
		this(META_CONTENT_ATTR);
	}

	public FlancoProductParser(String metaContentAttr) {
		super();
		this.metaContentAttr = metaContentAttr;
	}

	@Override
	protected String parseProductMeta(String url, WebPage page, Document document, String metaSelectors) {
		return ProductParserUtils.buildNameValuesString(ProductParserUtils.getAllNameValuesBySelector(document, metaSelectors));
	}

	@Override
	protected String parseProductPriceCurrency(String url, WebPage page, Document document, String selector) {
		return ProductParserUtils.extractFirstAttrText(document, metaContentAttr, selector);
	}
}