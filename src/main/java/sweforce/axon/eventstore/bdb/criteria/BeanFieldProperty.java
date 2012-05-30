//package sweforce.axon.eventstore.bdb.criteria;
//
//import org.apache.commons.beanutils.PropertyUtils;
//import org.axonframework.eventstore.management.Criteria;
//import org.axonframework.eventstore.management.Property;
//
//import java.lang.reflect.InvocationTargetException;
//import java.util.Comparator;
//
///**
// * Created with IntelliJ IDEA.
// * User: sveffa
// * Date: 5/30/12
// * Time: 9:34 AM
// * To change this template use File | Settings | File Templates.
// */
//public class BeanFieldProperty implements Property {
//
//    private final String propertyName;
//
//    public BeanFieldProperty(String propertyName) {
//        this.propertyName = propertyName;
//    }
//
//    public Object getProperty(Object o){
//        try{
//            return PropertyUtils.getProperty(o, propertyName);
//        }catch(IllegalAccessException acc){
//            throw new RuntimeException(acc);
//        }catch(IllegalArgumentException arg){
//            throw new RuntimeException(arg);
//        }catch(InvocationTargetException inv){
//            throw new RuntimeException(inv);
//        }catch(NoSuchMethodException method){
//            throw new RuntimeException(method);
//        }
//    }
//    /**
//     * @param o
//     * @return
//     */
//    @Override
//    public Criteria lessThan(Object o) {
//        Object propertyValue = getProperty(o);
//        if (o == null && propertyValue == null)
//            return false;
//        if (propertyValue != null)
//        return null;  //To change body of implemented methods use File | Settings | File Templates.
//    }
//
//    @Override
//    public Criteria lessThanEquals(Object o) {
//        return null;  //To change body of implemented methods use File | Settings | File Templates.
//    }
//
//    @Override
//    public Criteria greaterThan(Object o) {
//        return null;  //To change body of implemented methods use File | Settings | File Templates.
//    }
//
//    @Override
//    public Criteria greaterThanEquals(Object o) {
//        return null;  //To change body of implemented methods use File | Settings | File Templates.
//    }
//
//    @Override
//    public Criteria is(Object o) {
//        return null;  //To change body of implemented methods use File | Settings | File Templates.
//    }
//
//    @Override
//    public Criteria isNot(Object o) {
//        return null;  //To change body of implemented methods use File | Settings | File Templates.
//    }
//
//    @Override
//    public Criteria in(Object o) {
//        return null;  //To change body of implemented methods use File | Settings | File Templates.
//    }
//
//    @Override
//    public Criteria notIn(Object o) {
//        return null;  //To change body of implemented methods use File | Settings | File Templates.
//    }
//
//    public static abstract class SimpleComparableOperator extends BdbCriteria {
//        private final BeanFieldProperty property;
//
//        public SimpleComparableOperator(BeanFieldProperty property) {
//            this.property = property;
//        }
//
//        @Override
//        public boolean isCriteriaFulfilledFor(Object o) {
//            property.getProperty(o);
//
//            return false;  //To change body of implemented methods use File | Settings | File Templates.
//        }
//
//        protected abstract boolean evalCompare(int compareResult);
//
//    }
//}
