package com.evozon.mining.product.parsers;

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

	private final String productNameSelector;
	private final String productPriceWholeSelector;
	private final String productPricePartSelector;
	private final String productPriceCurrencySelector;
	private final String productDetailsSelector;

	public DefaultProductParser() {
		Map<String, String> selectorMap = initializeParser();

		productNameSelector = selectorMap.get(NAME);
		productPriceWholeSelector = selectorMap.get(PRICE_WHOLE);
		productPricePartSelector = selectorMap.get(PRICE_PART);
		productPriceCurrencySelector = selectorMap.get(PRICE_CURRENCY);
		productDetailsSelector = selectorMap.get(META);
	}

	public DefaultProductParser(String productNameSelector, String productPriceWholeSelector, String productPricePartSelector, String
			productPriceCurrencySelector, String productDetailsSelector) {
		this.productNameSelector = productNameSelector;
		this.productPriceWholeSelector = productPriceWholeSelector;
		this.productPricePartSelector = productPricePartSelector;
		this.productPriceCurrencySelector = productPriceCurrencySelector;
		this.productDetailsSelector = productDetailsSelector;
	}

	/**
	 * Default parsing implementation: by default this method would try to parse by extracting the
	 * data by the configured selectors
	 *
	 * @param url  the url currently being parsed
	 * @param page the page currently being parsed
	 */
	@Override
	public boolean parse(String url, WebPage page) {
		if (StringUtils.isBlank(productNameSelector)) {
			return false;
		}

		LOG.trace("Extracting product from URL '{}'", url);

		long now = System.currentTimeMillis();
		Document document = Jsoup.parse(new String(page.getContent().array()));

		LOG.trace("Content parsing duration: {}ms", System.currentTimeMillis() - now);

		now = System.currentTimeMillis();

		String productName = parseProductName(url, page, document, productNameSelector);
		if (StringUtils.isBlank(productName)) {
			return false;
		}

		Double price = parseProductPrice(url, page, document, productPriceWholeSelector, productPricePartSelector);
		if (price == null) {
			return false;
		}

		String priceCurrency = parseProductPriceCurrency(url, page, document, productPriceCurrencySelector);
		if (StringUtils.isBlank(priceCurrency)) {
			return false;
		}

		String productDetails = parseProductMeta(url, page, document, productDetailsSelector);

		LOG.trace("Full data extraction duration: {}ms", System.currentTimeMillis() - now);

		Map<CharSequence, ByteBuffer> metadata = page.getMetadata();

		metadata.put(ProductParserConstants.META_KEY_PRODUCT_NAME, ByteBuffer.wrap(productName.getBytes()));
		metadata.put(ProductParserConstants.META_KEY_PRODUCT_PRICE, ByteBuffer.wrap(ProductParserUtils.toByteArray(price)));
		metadata.put(ProductParserConstants.META_KEY_PRODUCT_CURRENCY, ByteBuffer.wrap(priceCurrency.getBytes()));
		if (productDetails.length() > 0) {
			metadata.put(ProductParserConstants.META_KEY_PRODUCT_DETAILS, ByteBuffer.wrap(productDetails.getBytes()));
		}

		if (LOG.isTraceEnabled()) {
			LOG.trace(">>>> Extracted product [ '{}' : {}{} ] from {} \n\tDetails:{}", productName, price, priceCurrency, url,
					productDetails);
		} else {
			LOG.info(">>>> Extracted product [ '{}' : {}{} ] from '{}'", productName, price, priceCurrency, url);
		}

		return true;
	}

	public String getProductNameSelector() {
		return productNameSelector;
	}

	public String getProductPriceWholeSelector() {
		return productPriceWholeSelector;
	}

	public String getProductPricePartSelector() {
		return productPricePartSelector;
	}

	public String getProductPriceCurrencySelector() {
		return productPriceCurrencySelector;
	}

	public String getProductDetailsSelector() {
		return productDetailsSelector;
	}

	/**
	 * Initialization method for the parser: here the parser can load its configuration up.
	 * The plugins typically would call this method in their static config loading process
	 */
	protected Map<String, String> initializeParser() {
		Map<String, String> selectorMap = new HashMap<>();
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

				String value = p.getProperty(key);
				selectorMap.put(key.toLowerCase().trim(), value.trim());
				LOG.debug(">>> '{}' / [ '{}':'{}' ]", this.getClass().getSimpleName(), key, value);
			}
		} catch (Exception e) {
			if (LOG.isErrorEnabled()) {
				LOG.error(e.toString());
			}
		}

		return selectorMap;
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
