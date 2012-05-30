package sweforce.bdb.util;

import com.sleepycat.je.Environment;
import com.sleepycat.persist.EntityStore;
import com.sleepycat.persist.StoreConfig;

/**
 * Created with IntelliJ IDEA.
 * User: sveffa
 * Date: 5/30/12
 * Time: 6:37 AM
 * To change this template use File | Settings | File Templates.
 */
public class EntityStoreFactory {


    private Environment environment;

    private StoreConfig storeConfig;


    public EntityStore createEntityStore(String dbname){
        EntityStore entityStore = new EntityStore(environment, dbname, storeConfig);
        return entityStore;
    }



}
