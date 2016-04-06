package com.evozon.mining.product.parsers;

import com.evozon.mining.product.extractor.nutch.ProductParser;
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

public abstract class DefaultProductParser implements ProductParser {

	private static final Logger LOG = LoggerFactory.getLogger(DefaultProductParser.class);
	private static Map<String, Map<String, String>> PRODUCT_PARSE_MAP = new HashMap<>();

	private String host;

	@Override
	public String getHost() {
		return host;
	}

	@Override
	public void initializeParser(String host) {
		this.host = host;

		Properties p = new Properties();

		try {
			p.load(this.getClass().getResourceAsStream("parser-mappings.properties"));
			Enumeration<?> keys = p.keys();
			while (keys.hasMoreElements()) {
				String key = (String) keys.nextElement();
				if (StringUtils.isBlank(key) || StringUtils.isBlank(p.getProperty(key))) {
					LOG.error("Invalid config entry '{}:{}'", key, p.getProperty(key));
					continue;
				}

				String selectorKey = key.toLowerCase().trim();

				String selector = p.getProperty(key).trim();
				Map<String, String> parseTokenMap = PRODUCT_PARSE_MAP.get(host);
				if (parseTokenMap == null) {
					parseTokenMap = new HashMap<>();
					PRODUCT_PARSE_MAP.put(host, parseTokenMap);
				}

				parseTokenMap.put(selectorKey, selector);
			}
		} catch (Exception e) {
			if (LOG.isErrorEnabled()) {
				LOG.error(e.toString());
			}
		}
	}

	@Override
	public void parse(String url, WebPage page) {
		Map<String, String> parserStrMap = PRODUCT_PARSE_MAP.get(host.toLowerCase());
		if (parserStrMap == null || parserStrMap.isEmpty()) {
			return;
		}

		String nameParser = parserStrMap.get(NAME);
		String priceWholeParser = parserStrMap.get(PRICE_WHOLE);
		String pricePartParser = parserStrMap.get(PRICE_PART);
		String metaParser = parserStrMap.get(META);

		if (StringUtils.isBlank(nameParser)) {
			return;
		}

		LOG.trace("+++ Extracting product from URL: " + url);

		long now = System.currentTimeMillis();
		Document document = Jsoup.parse(new String(page.getContent().array()));

		LOG.trace("Content parsing duration: {}ms", System.currentTimeMillis() - now);

		now = System.currentTimeMillis();

		String productName = extractText(document, nameParser);
		if (StringUtils.isBlank(productName)) {
			return;
		}

		String priceWhole = extractText(document, priceWholeParser).replaceAll("[^\\d.]", "");
		String pricePart = extractText(document, pricePartParser).replaceAll("[^\\d.]", "");
		if (StringUtils.isBlank(priceWhole)) {
			return;
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

		if (StringUtils.isBlank(productName) || price == null) {
			return;
		}

		LOG.trace("Full data extraction duration: %sms", System.currentTimeMillis() - now);

		StringBuilder productDetails = new StringBuilder();
		Elements productDetailsElement = document.select(metaParser);
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

		Map<CharSequence, ByteBuffer> metadata = page.getMetadata();
		LOG.debug("\n\t>>>> Storing product info [ " + productName + " ]");
		metadata.put(new Utf8(PRODUCT_KEY), ByteBuffer.wrap(productName.toString().getBytes()));
		metadata.put(new Utf8(PRODUCT_PRICE), ByteBuffer.wrap(toByteArray(price)));

		if (productDetails.length() > 0) {
			metadata.put(new Utf8(PRODUCT_DETAILS), ByteBuffer.wrap(productDetails.toString().getBytes()));
		}
	}

	private static String extractText(Document document, String selector) {
		String ret = "";
		Elements elements = document.select(selector);
		if (elements != null && elements.size() > 0) {
			Element element = elements.get(0);
			for (Node child : element.childNodes()) {
				ret = child.toString();
				if (child instanceof TextNode) {
					ret = ((TextNode) child).text();
				}
				break;
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
