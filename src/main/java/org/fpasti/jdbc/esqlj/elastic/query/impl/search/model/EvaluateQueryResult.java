package org.fpasti.jdbc.esqlj.elastic.query.impl.search.model;

import java.util.ArrayList;
import java.util.List;

import co.elastic.clients.elasticsearch._types.query_dsl.Query;

/**
* @author  Fabrizio Pasti - fabrizio.pasti@gmail.com
*/

public class EvaluateQueryResult {
		private boolean and = true;
		private List<Query> queryBuilders = new ArrayList<Query>();
		private List<Query> notQueryBuilders = new ArrayList<Query>();
		private TermsQuery termsQuery = new TermsQuery();
		private boolean reverseNegateOnNot;
		
		public EvaluateQueryResult() {
		}

		public EvaluateQueryResult(Query queryBuilder) {
			addQueryBuilder(queryBuilder);
		}

		public List<Query> getQueryBuilders() {
			return queryBuilders;
		}

		public List<Query> getNotQueryBuilders() {
			return notQueryBuilders;
		}

		public void addQueryBuilder(Query queryBuilder) {
			queryBuilders.add(queryBuilder);
		}
		
		public boolean isListEmpty() {
			return queryBuilders.size() == 0;
		}

		public boolean isNotListEmpty() {
			return notQueryBuilders.size() == 0;
		}

		public boolean isTermsEmpty() {
			return termsQuery.isEmpty();
		}

		public TermsQuery getTermsQuery() {
			return termsQuery;
		}
		
		public void addEqualTerm(String term, Object value) {
			termsQuery.addEqualObject(term, value);
		}

		public void addNotEqualTerm(String term, Object value) {
			termsQuery.addNotEqualObject(term, value);
		}
		
		public void addEqualTerms(String term, List<Object> values) {
			termsQuery.addEqualObjects(term, values);
		}

		public EvaluateQueryResult merge(boolean and, EvaluateQueryResult resAndRight) {
			this.and = and;
			queryBuilders.addAll(resAndRight.getQueryBuilders());
			notQueryBuilders.addAll(resAndRight.getNotQueryBuilders());
			termsQuery.merge(resAndRight.getTermsQuery());
			return this;
		}
		
		
		public EvaluateQueryResultType getType() {
			if(queryBuilders.size() == 1 && isNotListEmpty() && isTermsEmpty()) {
				return EvaluateQueryResultType.ONLY_ONE;
			}
			
			if(isListEmpty() && notQueryBuilders.size() == 1 && isTermsEmpty()) {
				return EvaluateQueryResultType.ONLY_ONE_NOT;
			}
			
			if(isListEmpty() && isNotListEmpty() && !isTermsEmpty()) {
				if(termsQuery.getEqualObjects().size() == 1 && termsQuery.getNotEqualObjects().isEmpty()) {
					return EvaluateQueryResultType.ONLY_ONE_TERMS;
				}
				
				if(termsQuery.getEqualObjects().isEmpty() && termsQuery.getNotEqualObjects().size() == 1) {
					return EvaluateQueryResultType.ONLY_ONE_NOT_TERMS;
				}
			}
			
			return EvaluateQueryResultType.MIXED;

		}

		public boolean isAnd() {
			return and;
		}

		public boolean isReverseNegateOnNot() {
			return reverseNegateOnNot;
		}

		public void setReverseNegateOnNot(boolean reverseNegateOnNot) {
			this.reverseNegateOnNot = reverseNegateOnNot;
		}
	
	}