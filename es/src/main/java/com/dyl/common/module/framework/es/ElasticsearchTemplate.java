package com.dyl.common.module.framework.es;

import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.deletebyquery.DeleteByQueryAction;
import org.elasticsearch.action.deletebyquery.DeleteByQueryRequestBuilder;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.reindex.AbstractBulkByScrollRequest;
import org.elasticsearch.index.reindex.UpdateByQueryAction;
import org.elasticsearch.index.reindex.UpdateByQueryRequestBuilder;
import org.elasticsearch.script.Script;
import org.elasticsearch.script.ScriptService;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;

/**
 * @Title: ElasticsearchTemplate.java
 * @Description: ElasticsearchTemplate
 * @author YanLong.Dong
 * @date 2017-11-16 下午4:53:22
 * @version Copyright © 2017 Deppon. All rights reserved
 */
public class ElasticsearchTemplate{

	private ElasticsearchTemplatePool elasticsearchTemplatePool;

	public void deleteByQuery()throws Exception{
		TransportClient t= getTransportClient();
		new DeleteByQueryRequestBuilder(t,   
				DeleteByQueryAction.INSTANCE)
		.setIndices("tvs_link_firstload1")
		.setTypes("link_firstload1")
		.setQuery(QueryBuilders.boolQuery().must(QueryBuilders.termQuery("bill_no", "31100"))
				.must(QueryBuilders.termQuery("waybill_no", "31100")))
		.execute()
		.actionGet();
		releaseTransportClient(t);
	}
	
	public void updateByQuery(String index,String type)throws Exception{
		TransportClient t= getTransportClient();
		UpdateByQueryRequestBuilder updateByQueryRequestBuilder = 
				UpdateByQueryAction.INSTANCE.newRequestBuilder(t);
		updateByQueryRequestBuilder.source("tvs_link_firstload1").source().setTypes("link_firstload1");
		updateByQueryRequestBuilder.filter(QueryBuilders.boolQuery().must(QueryBuilders.termQuery("bill_no", "80100"))).
		size(AbstractBulkByScrollRequest.SIZE_ALL_MATCHES).
		script(new Script("ctx._source.waybill_no=10000",ScriptService.ScriptType.INLINE, null, null)).execute()
		.get();
		releaseTransportClient(t);
	}	

	public boolean delete(String index,String type,String id)throws Exception {
		TransportClient t= getTransportClient();
		try {
			DeleteResponse dResponse = t.prepareDelete(index, type, id).execute().actionGet();
			boolean flag = dResponse.isFound();
			return flag;
		} finally {
			releaseTransportClient(t);
		}
	}

	public void query()throws Exception {
		TransportClient t= getTransportClient();
		try {
			SearchResponse actionGet = t.prepareSearch("tvs_link_firstload1")
					.setTypes("link_firstload1")
					.setQuery(QueryBuilders.termQuery("waybill_no","13100")
							).execute().actionGet();
			SearchHits hits = actionGet.getHits();
			List<Map<String, Object>> matchRsult = new LinkedList<Map<String, Object>>();
			for (SearchHit hit : hits.getHits()){
				matchRsult.add(hit.getSource());
			}
			System.out.println(matchRsult);
		} finally {
			releaseTransportClient(t);
		}
	}

	public void save(String index,String type,String id,String data)throws Exception {
		TransportClient t= getTransportClient();
		try {
			t.prepareIndex(index, type,id).
			setSource(data).execute().actionGet();
		} finally {
			releaseTransportClient(t);
		}
	}

	public void update(String index,String type,String id,String data)throws Exception {
		TransportClient t= getTransportClient();
		try {
			UpdateRequest  updateRequest = new UpdateRequest(index, type, id);
			updateRequest.script(new Script(data));
			t.update(updateRequest).get();
		} finally {
			releaseTransportClient(t);
		}
	}

	public void saveOrUpdate(String index,String type,String id,String data)throws Exception {
		TransportClient t= getTransportClient();
		try {
			IndexRequest indexRequest = new IndexRequest(index,type, id).source(data);
			UpdateRequest uRequest = new UpdateRequest(index,type, id).doc(data).upsert(indexRequest);
			t.update(uRequest).get();
		} finally {
			releaseTransportClient(t);
		}
	}

	private TransportClient getTransportClient() throws Exception{
		TransportClient t =elasticsearchTemplatePool.getTransportClientPool().get();
		return t;
	} 

	private void releaseTransportClient(TransportClient t) {
		elasticsearchTemplatePool.getTransportClientPool().release(t);
	}
	
	public void setElasticsearchTemplatePool(ElasticsearchTemplatePool elasticsearchTemplatePool) {
		this.elasticsearchTemplatePool = elasticsearchTemplatePool;
	}
}
