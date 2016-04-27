package com.evozon.mining.product.indexwriter.elastic;

public interface ElasticConstants {
  public static final String ELASTIC_PREFIX = "elastic.";

  public static final String HOST = ELASTIC_PREFIX + "host";
  public static final String PORT = ELASTIC_PREFIX + "port";
  public static final String CLUSTER = ELASTIC_PREFIX + "cluster";
  public static final String INDEX = ELASTIC_PREFIX + "index";
  public static final String MAX_BULK_DOCS = ELASTIC_PREFIX + "max.bulk.docs";
  public static final String MAX_BULK_LENGTH = ELASTIC_PREFIX + "max.bulk.size";
}
