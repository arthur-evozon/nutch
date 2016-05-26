package com.evozon.mining.product.parsers;

import org.apache.avro.util.Utf8;
import org.apache.commons.lang3.StringUtils;
import org.apache.nutch.storage.WebPage;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.nodes.Node;
import org.jsoup.select.Elements;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

/**
 * Default {@link ProductParser} implementation
 */
public class DefaultProductParser implements ProductParser {
	private static final Logger LOG = LoggerFactory.getLogger(DefaultProductParser.class);
	public static final String PARSERS_CONFIGURATION_FILE = "parser-mappings.properties";

	public static final String NAME = "product-name";
	public static final String PRICE_WHOLE = "product-price/whole";
	public static final String PRICE_PART = "product-price/part";
	public static final String PRICE_CURRENCY = "product-price/currency";
	public static final String META = "product-meta";

	public static final String SELECTOR_SEPARATOR = "|";

	Map<String, String> selectorMap = new HashMap<>();

	/**
	 * The plugins typically would call this method in their static config loading process
	 */
	@Override
	public void initializeParser() {
		Properties p = new Properties();
		try {
			p.load(this.getClass().getResourceAsStream(PARSERS_CONFIGURATION_FILE));
			Enumeration<?> keys = p.keys();
			while (keys.hasMoreElements()) {
				String key = (String) keys.nextElement();
				if (StringUtils.isBlank(key) || StringUtils.isBlank(p.getProperty(key))) {
					LOG.error("Invalid config entry '{}:{}'", key, p.getProperty(key));
					continue;
				}

				selectorMap.put(key.toLowerCase().trim(), p.getProperty(key).trim());
			}
		} catch (Exception e) {
			if (LOG.isErrorEnabled()) {
				LOG.error(e.toString());
			}
		}
	}

	/**
	 * Default parsing implementation: by default this method would try to parse by extracting the
	 * data by the configured selectors
	 *
	 * @param url  the url currently being parsed
	 * @param page the page currently being parsed
	 */
	@Override
	public void parse(String url, WebPage page) {
		String nameSelector = selectorMap.get(NAME);
		String priceWholeSelector = selectorMap.get(PRICE_WHOLE);
		String pricePartSelector = selectorMap.get(PRICE_PART);
		String priceCurrencySelector = selectorMap.get(PRICE_CURRENCY);
		String productDetailsSelector = selectorMap.get(META);

		if (StringUtils.isBlank(nameSelector)) {
			return;
		}

		LOG.trace("Extracting product from URL '{}'", url);

		long now = System.currentTimeMillis();
		Document document = Jsoup.parse(new String(page.getContent().array()));

		LOG.trace("Content parsing duration: {}ms", System.currentTimeMillis() - now);

		now = System.currentTimeMillis();

		String productName = parseProductName(url, page, document, nameSelector);
		if (StringUtils.isBlank(productName)) {
			return;
		}

		Double price = parseProductPrice(url, page, document, priceWholeSelector, pricePartSelector);
		if (price == null) {
			return;
		}

		String priceCurrency = parseProductPriceCurrency(url, page, document, priceCurrencySelector);
		if (StringUtils.isBlank(priceCurrency)) {
			return;
		}

		String productDetails = parseProductMeta(url, page, document, productDetailsSelector);

		LOG.trace("Full data extraction duration: {}ms", System.currentTimeMillis() - now);

		Map<CharSequence, ByteBuffer> metadata = page.getMetadata();

		metadata.put(new Utf8(ProductParserConstants.PRODUCT_KEY), ByteBuffer.wrap(productName.getBytes()));
		metadata.put(new Utf8(ProductParserConstants.PRODUCT_PRICE), ByteBuffer.wrap(ProductParserUtils.toByteArray(price)));
		metadata.put(new Utf8(ProductParserConstants.PRODUCT_CURRENCY), ByteBuffer.wrap(priceCurrency.getBytes()));
		if (productDetails.length() > 0) {
			metadata.put(new Utf8(ProductParserConstants.PRODUCT_DETAILS), ByteBuffer.wrap(productDetails.getBytes()));
		}

		LOG.debug("\n\t>>>> Stored product [ '{}' : {}{} ] \n\t{}", productName, price, priceCurrency, productDetails);
	}

	protected String parseProductName(String url, WebPage page, Document document, String selector) {
		return ProductParserUtils.extractFirstChildText(document, selector);
	}

	protected Double parseProductPrice(String url, WebPage page, Document document, String priceWholeSelector, String pricePartSelector) {
		String priceWhole = ProductParserUtils.extractFirstChildText(document, priceWholeSelector).replaceAll("[^\\d]", "");
		String pricePart = ProductParserUtils.extractFirstChildText(document, pricePartSelector).replaceAll("[^\\d]", "");
		if (StringUtils.isBlank(priceWhole)) {
			return null;
		}

		return ProductParserUtils.buildPrice(priceWhole, pricePart);
	}

	protected String parseProductPriceCurrency(String url, WebPage page, Document document, String selector) {
		return ProductParserUtils.extractFirstChildText(document, selector);
	}

	protected String parseProductMeta(String url, WebPage page, Document document, String metaSelectors) {
		StringBuilder productDetails = new StringBuilder();

		String[] selectors = new String[]{metaSelectors};

		if (metaSelectors.contains(SELECTOR_SEPARATOR)) {
			selectors = metaSelectors.split(SELECTOR_SEPARATOR);
		}

		for (String selector : selectors) {
			Elements productDetailsElement = document.select(selector.trim());
			if (productDetailsElement != null) {
				for (Element element : productDetailsElement) {
					for (Node child : element.childNodes()) {
						String[] details = child.toString().split(",");
						for (String detail : details) {
							if (productDetails.length() > 0) {
								productDetails.append("\n");
							}

							productDetails.append(detail.trim());
						}
					}
				}
			}
		}

		return productDetails.toString();
	}
}
