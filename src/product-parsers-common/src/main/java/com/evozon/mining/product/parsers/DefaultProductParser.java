package com.evozon.mining.product.parsers;

import org.apache.avro.util.Utf8;
import org.apache.commons.lang3.StringUtils;
import org.apache.nutch.storage.WebPage;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.nodes.Node;
import org.jsoup.nodes.TextNode;
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

		LOG.trace("Extracting product from URL '{}'",url);

		long now = System.currentTimeMillis();
		Document document = Jsoup.parse(new String(page.getContent().array()));

		LOG.trace("Content parsing duration: {}ms", System.currentTimeMillis() - now);

		now = System.currentTimeMillis();

		String productName = parseProductName(url, page, document, nameSelector);
		if (StringUtils.isBlank(productName)) {
			return;
		}

		Double price = parseProductPrice(url,page,document, priceWholeSelector, pricePartSelector);
		if (price == null) {
			return;
		}

		String priceCurrency = parseProductPriceCurrency(url,page,document,priceCurrencySelector);
		if( StringUtils.isBlank(priceCurrency)) {
			return;
		}

		String productDetails = parseProductMeta(url,page,document,productDetailsSelector);

		LOG.trace("Full data extraction duration: {}ms", System.currentTimeMillis() - now);

		Map<CharSequence, ByteBuffer> metadata = page.getMetadata();

		metadata.put(new Utf8(PRODUCT_KEY), ByteBuffer.wrap(productName.getBytes()));
		metadata.put(new Utf8(PRODUCT_PRICE), ByteBuffer.wrap(toByteArray(price)));
		metadata.put(new Utf8(PRODUCT_CURRENCY), ByteBuffer.wrap(priceCurrency.getBytes()));
		if (productDetails.length() > 0) {
			metadata.put(new Utf8(PRODUCT_DETAILS), ByteBuffer.wrap(productDetails.getBytes()));
		}

		LOG.debug("\n\t>>>> Stored product [ '{}' : {}{} ]",productName, price, priceCurrency );
	}

	String parseProductName(String url, WebPage page, Document document, String selector) {
		return extractText(document, selector);
	}

	Double parseProductPrice(String url, WebPage page, Document document, String priceWholeSelector, String pricePartSelector) {
		String priceWhole = extractText(document, priceWholeSelector).replaceAll("[^\\d.]", "");
		String pricePart = extractText(document, pricePartSelector).replaceAll("[^\\d.]", "");
		if (StringUtils.isBlank(priceWhole)) {
			return null;
		}

		Double price = null;
		try {
			if (StringUtils.isBlank(pricePart)) {
				pricePart = "0";
			}

			price = Double.parseDouble(String.format("%s.%s", priceWhole, pricePart));
		} catch (NumberFormatException e) {
			LOG.debug("Could not extract price from [{}.{}]", priceWhole, pricePart);
		}

		return price;
	}

	String parseProductPriceCurrency(String url, WebPage page, Document document, String selector) {
		return extractText(document, selector);
	}

	String parseProductMeta(String url, WebPage page, Document document, String selector) {
		StringBuilder productDetails = new StringBuilder();
		Elements productDetailsElement = document.select(selector);
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

		return productDetails.toString();
	}

	public static String extractText(Document document, String selector) {
		return extractText(document, selector, 1);
	}

	public static String extractText(Document document, String selector, int nth) {
		String ret = "";
		Elements elements = document.select(selector);
		if (elements != null && elements.size() > 0 ) {
			Element element = elements.get(0);
			for (Node child : element.childNodes()) {
				ret = child.toString();
				if (child instanceof TextNode) {
					ret = ((TextNode) child).text();
				}

				if( --nth <= 0 ) {
					break;
				}
			}
		}

		return ret;
	}

	public static byte[] toByteArray(double value) {
		byte[] bytes = new byte[8];
		ByteBuffer.wrap(bytes).putDouble(value);
		return bytes;
	}

	public static double toDouble(byte[] bytes) {
		return ByteBuffer.wrap(bytes).getDouble();
	}
}
