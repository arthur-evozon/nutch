package com.evozon.mining.product.extractor.nutch;

import com.evozon.mining.product.parsers.ProductParser;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.nutch.parse.HTMLMetaTags;
import org.apache.nutch.parse.Parse;
import org.apache.nutch.parse.ParseFilter;
import org.apache.nutch.storage.WebPage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.w3c.dom.DocumentFragment;

import java.net.MalformedURLException;
import java.net.URL;
import java.util.*;

/**
 * The parse portion of the Tag Extractor module. Parses out blog tags
 * from the body of the document and sets it into the ParseResult object.
 */
public class ProductExtractorParseFilter implements ParseFilter {
	private static final Logger LOG = LoggerFactory.getLogger(ProductExtractorParseFilter.class);

	private static final Collection<WebPage.Field> FIELDS = new HashSet<>();
	private static final List<String> htmlMimeTypes = Arrays.asList(new String[]{"text/html", "application/xhtml+xml"});
	static final Map<String, ProductParser> PARSER_MAP = new HashMap<>();

	private Configuration conf;

	static {
		FIELDS.add(WebPage.Field.CONTENT);

		try {
			Properties p = new Properties();
			p.load(ProductExtractorParseFilter.class.getResourceAsStream("product-parser.properties"));
			Enumeration<?> keys = p.keys();
			while (keys.hasMoreElements()) {
				String key = (String) keys.nextElement();
				if (StringUtils.isBlank(key) || StringUtils.isBlank(p.getProperty(key))) {
					LOG.error("Invalid config entry '{}:{}'", key, p.getProperty(key));
					continue;
				}

				String host = key.toLowerCase().trim();
				String implementation = p.getProperty(key).trim();
				Class c = Class.forName(implementation);
				ProductParser productParser = (ProductParser) c.newInstance();

				PARSER_MAP.put(host, productParser);
			}
		} catch (Exception e) {
			if (LOG.isErrorEnabled()) {
				LOG.error(e.toString());
			}
		}
	}

	@Override
	public Configuration getConf() {
		return conf;
	}

	@Override
	public void setConf(Configuration conf) {
		this.conf = conf;
	}

	@Override
	public Collection<WebPage.Field> getFields() {
		return FIELDS;
	}

	@Override
	public Parse filter(String url, WebPage page, Parse parse, HTMLMetaTags metaTags, DocumentFragment doc) {
		String host;
		try {
			URL u = new URL(url);
			host = u.getHost();
		} catch (MalformedURLException e) {
			return parse;
		}

		ProductParser productParser = PARSER_MAP.get(host.trim().toLowerCase());
		if (productParser != null) {
			LOG.trace("Found parsers for {}: '{}'", host, productParser.getClass().getName());
			boolean productParsed = productParser.parse(url, page);

			if (productParsed) {
				LOG.debug("Successfully parsed product for '{}'", url);
			}
		}

		return parse;
	}
}