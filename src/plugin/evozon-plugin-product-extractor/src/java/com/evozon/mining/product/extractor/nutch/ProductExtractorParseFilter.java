package com.evozon.mining.product.extractor.nutch;

import org.apache.avro.util.Utf8;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.nutch.parse.HTMLMetaTags;
import org.apache.nutch.parse.Parse;
import org.apache.nutch.parse.ParseFilter;
import org.apache.nutch.storage.WebPage;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.nodes.Node;
import org.jsoup.nodes.TextNode;
import org.jsoup.select.Elements;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.w3c.dom.DocumentFragment;

import java.net.MalformedURLException;
import java.net.URL;
import java.nio.ByteBuffer;
import java.util.*;

/**
 * The parse portion of the Tag Extractor module. Parses out blog tags
 * from the body of the document and sets it into the ParseResult object.
 */
public class ProductExtractorParseFilter implements ParseFilter {
	public static final String PRODUCT_KEY = "product-name";
	public static final String PRODUCT_PRICE = "product-price";
	public static final String PRODUCT_DETAILS = "product-meta";

	private static final Logger LOG = LoggerFactory.getLogger(ProductExtractorParseFilter.class);

	public static final String PARSE_KEY_SEPARATOR = "\\|";
	public static final String NAME = "name";
	public static final String PRICE_WHOLE = "price-whole";
	public static final String PRICE_PART = "price-part";
	public static final String META = "meta";


	private static final List<String> htmlMimeTypes = Arrays.asList(new String[]{"text/html", "application/xhtml+xml"});

	// Configuration
	private Configuration configuration;
	private String defaultEncoding;

	private static Map<String, Map<String, String>> PRODUCT_PARSE_MAP = new HashMap<>();

	private static final Collection<WebPage.Field> FIELDS = new HashSet<>();

	static {
		FIELDS.add(WebPage.Field.CONTENT);

		try {
			Properties p = new Properties();
			p.load(ProductExtractorParseFilter.class.getResourceAsStream("parser-mappings.properties"));
			Enumeration<?> keys = p.keys();
			while (keys.hasMoreElements()) {
				String key = (String) keys.nextElement();
				if (StringUtils.isBlank(key) || StringUtils.isBlank(p.getProperty(key))) {
					LOG.error("Invalid config entry '{}:{}'", key, p.getProperty(key));
					continue;
				}

				String[] keyPair = key.split(PARSE_KEY_SEPARATOR);
				if (keyPair == null || keyPair.length != 2) {
					LOG.error("Invalid config entry '{}:{}'", key, p.getProperty(key));
					continue;
				}

				String host = keyPair[0].toLowerCase().trim();
				String selectorKey = keyPair[1].toLowerCase().trim();

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


	private Configuration conf;

	/**
	 * We use regular expressions to parse out the Labels section from
	 * the section snippet shown below:
	 * <pre>
	 * Labels:
	 * <a href='http://sujitpal.blogspot.com/search/label/ror' rel='tag'>ror</a>,
	 * ...
	 * </span>
	 * </pre>
	 * Accumulate the tag values into a List, then stuff the list into the
	 * parseResult with a well-known key (exposed as a public static variable
	 * here, so the indexing filter can pick it up from here).
	 */

	@Override
	public Parse filter(String url, WebPage page, Parse parse, HTMLMetaTags metaTags, DocumentFragment doc) {
		String host = null;
		try {
			URL u = new URL(url);
			host = u.getHost();
		} catch (MalformedURLException e) {
			return parse;
		}

		LOG.trace("Loading parsers for {}", host);

		Map<String, String> parserStrMap = PRODUCT_PARSE_MAP.get(host.toLowerCase());
		if (parserStrMap == null || parserStrMap.isEmpty()) {
			return parse;
		}

		String nameParser = parserStrMap.get(NAME);
		String priceWholeParser = parserStrMap.get(PRICE_WHOLE);
		String pricePartParser = parserStrMap.get(PRICE_PART);
		String metaParser = parserStrMap.get(META);

		if (StringUtils.isBlank(nameParser)) {
			return parse;
		}

		LOG.trace("+++ Extracting product from URL: " + url);

		long now = System.currentTimeMillis();
		Document document = Jsoup.parse(new String(page.getContent().array()));

		LOG.trace( "Content parsing duration: %sms", System.currentTimeMillis() - now );

		now = System.currentTimeMillis();

		String productName = extractText(document, nameParser);
		if (StringUtils.isBlank(productName)) {
			return parse;
		}

		String priceWhole = extractText(document, priceWholeParser).replaceAll("[^\\d.]", "");
		String pricePart = extractText(document, pricePartParser).replaceAll("[^\\d.]", "");
		if (StringUtils.isBlank(priceWhole)) {
			return parse;
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
			return parse;
		}

		LOG.trace( "Full data extraction duration: %sms", System.currentTimeMillis() - now );

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

		return parse;
	}

	public Configuration getConf() {
		return conf;
	}

	public void setConf(Configuration conf) {
		this.conf = conf;
	}

	@Override
	public Collection<WebPage.Field> getFields() {
		return FIELDS;
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