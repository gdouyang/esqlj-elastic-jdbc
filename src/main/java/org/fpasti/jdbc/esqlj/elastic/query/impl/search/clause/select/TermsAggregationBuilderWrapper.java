package org.fpasti.jdbc.esqlj.elastic.query.impl.search.clause.select;

import java.util.LinkedHashMap;
import java.util.Map;
import co.elastic.clients.elasticsearch._types.SortOrder;
import co.elastic.clients.elasticsearch._types.aggregations.Aggregation;
import co.elastic.clients.elasticsearch._types.aggregations.Aggregation.Builder.ContainerBuilder;
import co.elastic.clients.elasticsearch._types.aggregations.TermsAggregation;
import co.elastic.clients.util.NamedValue;

public class TermsAggregationBuilderWrapper {
  private TermsAggregation.Builder builder;
  
  private Map<String, Object> aggregations;
  
  public TermsAggregationBuilderWrapper(TermsAggregation.Builder builder) {
    this.builder = builder;
    this.aggregations = new LinkedHashMap<>();
  }
  
  public void order(NamedValue<SortOrder> value, @SuppressWarnings("unchecked") NamedValue<SortOrder>... values) {
    this.builder.order(value, values);
  }
  
  public void aggregations(String name, TermsAggregation.Builder aggregation) {
    this.aggregations.put(name, aggregation);
  }
  
  public void aggregations(String name, Aggregation aggregation) {
    this.aggregations.put(name, aggregation);
  }
  
  public Aggregation build() {
    ContainerBuilder b = new Aggregation.Builder().terms(builder.build());
    aggregations.forEach((name, agg) -> {
      if (agg instanceof TermsAggregation.Builder) {
        b.aggregations(name, ((TermsAggregation.Builder)agg).build()._toAggregation());
      } else {
        b.aggregations(name, (Aggregation)agg);      
      }
    });
    return b.build();
  }
  
  
}
