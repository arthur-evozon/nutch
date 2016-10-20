package com.evozon.mining.product.parsers.altex;

import com.evozon.mining.product.parsers.DefaultProductParser;
import com.evozon.mining.product.parsers.ProductParser;
import com.evozon.mining.product.parsers.ProductParserUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.nutch.storage.WebPage;
import org.jsoup.nodes.Document;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class AltexProductParser extends DefaultProductParser implements ProductParser {
	private static final Logger LOG = LoggerFactory.getLogger(AltexProductParser.class);
	static final String META_CONTENT_ATTR = "content";

	private final String metaContentAttr;

	public AltexProductParser() {
		this(META_CONTENT_ATTR);
	}

	public AltexProductParser(String metaContentAttr) {
		this.metaContentAttr = metaContentAttr;
	}

	@Override
	protected String parseProductMeta(String url, WebPage page, Document document, String metaSelectors) {
		return ProductParserUtils.buildNameValuesString(ProductParserUtils.getAllNameValuesBySelector(document, metaSelectors));
	}

	@Override
	protected Double parseProductPrice(String url, WebPage page, Document document, String priceWholeSelector, String pricePartSelector) {
		String priceStr = ProductParserUtils.extractFirstAttrText(document, metaContentAttr, priceWholeSelector);
		if (StringUtils.isBlank(priceStr)) {
			return null;
		}

		// replace html '&nbsp;'es with " "
		priceStr = priceStr.replaceAll("\u00a0", " ").replaceAll("[^\\d]", "");

		return Double.parseDouble(priceStr);
	}

	@Override
	protected String parseProductPriceCurrency(String url, WebPage page, Document document, String selector) {
		return ProductParserUtils.extractFirstAttrText(document, metaContentAttr, selector);
	}
}