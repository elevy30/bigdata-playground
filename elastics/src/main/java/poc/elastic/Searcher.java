package poc.elastic;

import lombok.extern.slf4j.Slf4j;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.query.RangeQueryBuilder;
import org.elasticsearch.index.query.TermQueryBuilder;
import org.elasticsearch.search.SearchHit;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Created by eyallevy on 19/08/17 .
 */
@Slf4j
class Searcher {

    List<Long> searchAnDsList(Client client, String index, String type, TermQueryBuilder queryBuilder) {
        List<Long> dsIdList = new ArrayList<>();
        SearchResponse response = client.prepareSearch(index).setTypes(type)
                //                .setSearchType(SearchType.DFS_QUERY_THEN_FETCH)
                .setQuery(queryBuilder)
                //                .setPostFilter(QueryBuilders.rangeQuery("age").from(12).to(18))     // Filter
                .setFetchSource(new String[]{"ds_row_id"}, null)
//                    .setFrom(0)
//                    .setSize(60)
//                    .setExplain(true)
                .execute().actionGet();


        assert response != null;
        SearchHit[] hits = response.getHits().getHits();
        for (SearchHit hit : hits) {
            final Map<String, Object> fields = hit.getSourceAsMap();
            Object ds_row_id = fields.get("ds_row_id");
            dsIdList.add((Long) ds_row_id);

        }
        log.debug("------------------------------");
        log.debug("result:{}", dsIdList);
        return dsIdList;
    }


    TimeWindow searchTimeWindow(Client client, String index, String type, TermQueryBuilder queryBuilder) {
        TimeWindow timeWindow = new TimeWindow();
        SearchResponse response = client.prepareSearch(index)
                .setTypes(type)
                .setQuery(queryBuilder)
                .setFetchSource(new String[]{"before_start_date", "after_end_date"}, null)
                .execute().actionGet();

        assert response != null;
        SearchHit[] hits = response.getHits().getHits();
        for (SearchHit hit : hits) {
            final Map<String, Object> fields = hit.getSourceAsMap();
            Long beforeStartDate = (Long) fields.get("before_start_date");
            Long after_end_date = (Long) fields.get("after_end_date");
            timeWindow = new TimeWindow(beforeStartDate, after_end_date);

        }
        log.debug("------------------------------");
        log.debug("result:{}", timeWindow);
        return timeWindow;
    }

    List<DSRecord> searchDsInWindow(Client client, String index, String type, RangeQueryBuilder queryBuilder, List<Long> anDsList, int size) {

        long counter = 1;
        long numOfSourceRecordEffectedAnomaly = 0;
        SearchResponse scrollResp = client.prepareSearch(index).setTypes(type)
                .setPostFilter(queryBuilder)
                .setScroll(new TimeValue(60000))
                .setSize(size)
                .setExplain(true)
                .execute().actionGet();

        do {
            List<DSRecord> dsList = new ArrayList<>();
            assert scrollResp != null;
            SearchHit[] hits = scrollResp.getHits().getHits();
            log.info("NUMBER OF RECORD : {}", hits.length);
            for (SearchHit hit : hits) {
                final Map<String, Object> fields = hit.getSourceAsMap();
                Long dsRowId = (Long) fields.get("ds_row_id");
                boolean effectedAnomaly;

                if(anDsList.contains(dsRowId)){
                    effectedAnomaly  = true;
                    numOfSourceRecordEffectedAnomaly++;
                }else{
                    effectedAnomaly = false;
                }

                DSRecord dsRecord = new DSRecord(effectedAnomaly, fields);
                dsList.add(dsRecord);
            }
            log.debug("------------------------------");
            for (DSRecord dsRecord : dsList) {
                log.debug(" -- " + dsRecord + " -- Record Number: " + counter + " -- ");
                counter++;
            }
            log.debug("");

            scrollResp = client.prepareSearchScroll(scrollResp.getScrollId()).setScroll(new TimeValue(60000)).execute().actionGet();
        } while(scrollResp.getHits().getHits().length != 0); // Zero hits mark the end of the scroll and the while loop.


        log.info("Number record effected Anomalies is :{}", numOfSourceRecordEffectedAnomaly);
        return null;
    }
}
