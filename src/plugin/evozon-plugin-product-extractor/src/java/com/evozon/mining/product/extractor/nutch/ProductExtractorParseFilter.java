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
import org.jsoup.select.Elements;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.w3c.dom.DocumentFragment;

import java.nio.ByteBuffer;
import java.util.*;

/**
 * The parse portion of the Tag Extractor module. Parses out blog tags
 * from the body of the document and sets it into the ParseResult object.
 */
public class ProductExtractorParseFilter implements ParseFilter {
	public static final String PRODUCT_KEY = "product-details";
	public static final String PRODUCT_DETAILS = "product-meta";

	private static final Logger LOG = LoggerFactory.getLogger(ProductExtractorParseFilter.class);

	public static final String NAME = "name";
	public static final String PRICE = "price";
	public static final String META = "meta";


	private static final List<String> htmlMimeTypes = Arrays.asList(new String[]{"text/html", "application/xhtml+xml"});

	// Configuration
	private Configuration configuration;
	private String defaultEncoding;

	private static Map<String, Map<String, String>> PRODUCT_PARSE_MAP = new HashMap<>();

	private static final Collection<WebPage.Field> FIELDS = new HashSet<>();

	static {
		FIELDS.add(WebPage.Field.METADATA);
		try {
			Properties p = new Properties();
			p.load(ProductExtractorParseFilter.class.getResourceAsStream("parser-mappings.properties"));
			Enumeration<?> keys = p.keys();
			while (keys.hasMoreElements()) {
				String key = (String) keys.nextElement();
				String[] values = p.getProperty(key).split(",", -1);
				for (int i = 0; i < values.length; i++) {
					String value = values[i].trim();
					if (StringUtils.isNotEmpty(value)) {
						String[] mapping = value.split(":");
						if (mapping != null && mapping.length == 2) {
							Map<String, String> parseTokenMap = PRODUCT_PARSE_MAP.get(key.trim().toLowerCase());
							if (parseTokenMap == null) {
								parseTokenMap = new HashMap<>();
								PRODUCT_PARSE_MAP.put(key.trim().toLowerCase(), parseTokenMap);
							}

							parseTokenMap.put(mapping[0].trim(), mapping[1].trim());
						}
					}
				}
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
		String host = page.getBaseUrl().toString();

		LOG.debug("+ Loading parsers for {}", host);

		Map<String, String> parserStrMap = PRODUCT_PARSE_MAP.get(host);
		if (parserStrMap == null || parserStrMap.isEmpty()) {
			return parse;
		}

		String nameParser = parserStrMap.get(NAME);
		String priceParser = parserStrMap.get(PRICE);
		String metaParser = parserStrMap.get(META);

		if (StringUtils.isBlank(nameParser)) {
			return parse;
		}

		LOG.trace("+++ Extracting product from URL: " + url);

		Document document = Jsoup.parse(new String(page.getContent().array()));

		StringBuilder productDefinition = new StringBuilder();
		Elements productElement = document.select(nameParser);
		if (productElement != null) {
			for (Element element : productElement) {
				for (Node child : element.childNodes()) {
					if (productDefinition.length() > 0) {
						productDefinition.append("\n");
					}

					productDefinition.append(child.toString().trim());
				}
			}
		}

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
		if (productDefinition.length() > 0) {
			LOG.debug("\n\t>>>>Storing product info [ " + productDefinition.toString() + " ]");
			metadata.put(new Utf8(PRODUCT_KEY), ByteBuffer.wrap(productDefinition.toString().getBytes()));
		}

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
		return null;
	}
}