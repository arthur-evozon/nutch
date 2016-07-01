package com.evozon.mining.product.parsers.evomag;

import com.evozon.mining.product.parsers.DefaultProductParser;
import com.evozon.mining.product.parsers.ProductParser;
import com.evozon.mining.product.parsers.ProductParserUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.nutch.storage.WebPage;
import org.jsoup.nodes.Document;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class EvomagProductParser extends DefaultProductParser implements ProductParser {
	private static final Logger LOG = LoggerFactory.getLogger(EvomagProductParser.class);
	static final String META_CONTENT_ATTR = "content";
	static final String[] DETAILS_NESTED_SELECT_PATTERN = new String[]{"tr", "td"};
	static final String PRICE_TOKEN_SEPARATOR = "\\.";

	private final String priceTokensSeparator;
	private final String metaContentAttr;
	private final String[] detailsNestedSelectPattern;

	public EvomagProductParser() {
		this(PRICE_TOKEN_SEPARATOR, META_CONTENT_ATTR, DETAILS_NESTED_SELECT_PATTERN);
	}

	public EvomagProductParser(String priceTokensSeparator, String metaContentAttr, String... detailsNestedSelectPattern) {
		this.priceTokensSeparator = priceTokensSeparator;
		this.metaContentAttr = metaContentAttr;
		this.detailsNestedSelectPattern = detailsNestedSelectPattern;
	}

	@Override
	protected Double parseProductPrice(String url, WebPage page, Document document, String priceWholeSelector, String pricePartSelector) {
		String priceStr = ProductParserUtils.extractFirstChildText(document, priceWholeSelector);
		if (StringUtils.isBlank(priceStr)) {
			return null;
		}

		String[] priceTokens = priceStr.split(priceTokensSeparator);
		if (priceTokens.length != 2) {
			return null;
		}

		String priceWhole = priceTokens[0].replaceAll("[^\\d]", "");
		String pricePart = priceTokens[1];

		return Double.parseDouble(String.format("%s.%s", priceWhole, pricePart));
	}

	@Override
	protected String parseProductPriceCurrency(String url, WebPage page, Document document, String selector) {
		return ProductParserUtils.extractFirstAttrText(document, metaContentAttr, selector);
	}

	@Override
	protected String parseProductMeta(String url, WebPage page, Document document, String metaSelectors) {
		return ProductParserUtils.buildNameValuesString(ProductParserUtils.getAllNameValuesWithSelectorPattern(document, metaSelectors,
				detailsNestedSelectPattern));
	}
}