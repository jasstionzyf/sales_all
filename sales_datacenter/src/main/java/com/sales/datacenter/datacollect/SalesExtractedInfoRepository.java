package com.sales.datacenter.datacollect;

import com.google.common.collect.Maps;
import com.yufei.extractor.core.ExtractedInfoRepository;
import com.yufei.extractor.entity.UfLink;
import com.yufei.pfw.entity.Entity;
import com.yufei.pfw.service.MongodbPfwService;
import com.yufei.pfw.service.PfwService;
import com.yufei.pfw.service.PfwServiceFactory;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.query.BasicQuery;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.data.mongodb.core.query.Update;

import java.util.Iterator;
import java.util.Map;

/**
 * Created by jasstion on 15/9/27.
 */
public class SalesExtractedInfoRepository implements ExtractedInfoRepository {

    private static ExtractedInfoRepository extractedInfoRepository = null;
    private PfwService pfwService = null;

    private SalesExtractedInfoRepository(String databaseName) {
        pfwService = PfwServiceFactory.getMongoPfwService(databaseName);
    }

    /**
     *
     * @param databasename
     * @return
     */
    public static ExtractedInfoRepository getInstance(String databasename) {
        if (extractedInfoRepository == null) {
            extractedInfoRepository = new SalesExtractedInfoRepository(databasename);
        }
        return extractedInfoRepository;
    }

    /**
     *
     * @param entity
     * @return
     */
    public Long saveInfo(Entity entity) {
        Long id = 0l;
        id = (Long) pfwService.save(entity);
        return id;
    }

    /**
     *
     * @param mallId
     * @param mallItemId
     * @return
     */
    public boolean isRepeatInfo(String mallId, String mallItemId) {
        boolean isRepeate = false;
        Map<String,Object> params=Maps.newHashMap();
        params.put("mallId", mallId);
        params.put("mallItemId", mallItemId);
        long num=pfwService.count(params, UfLink.class);
        if(num>0){
            return true;
        }
        return false;
    }

    /**
     *
     * @param link
     * @return
     */
    public UfLink getInfo(String link) {

        throw new UnsupportedOperationException();
    }

    /**
     *
     * @return
     */
    public Iterator<UfLink> iteratInfo() {
        final MongoTemplate mongoTemplate = ((MongodbPfwService) pfwService).getMongoTemplate();
        Iterator<UfLink> iterator = new Iterator<UfLink>() {
            public boolean hasNext() {

                return true;
            }

            public UfLink next() {
                final String field = "isProcessed";
                Query query = new BasicQuery("{" + field + ":false}");

                Update update = new Update();
                update.set(field, true);

                UfLink ufLink = mongoTemplate.findAndModify(query, update, UfLink.class);

                return ufLink;
            }
        };
        return iterator;

    }

    /**
     *
     * @param ufLink
     * @param isProcessed
     */
    public void updateIsProcessedStatus(UfLink ufLink, boolean isProcessed) {
        //  isProcessed
        ufLink.setIsProcessed(isProcessed);
        pfwService.update(ufLink);

    }

    @Override
    public void commitToSolr(Entity t) {
       
    }

   
  
}
