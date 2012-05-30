//package sweforce.axon.eventstore.bdb.criteria;
//
//import org.axonframework.eventstore.management.Criteria;
//
///**
// * Created with IntelliJ IDEA.
// * User: sveffa
// * Date: 5/30/12
// * Time: 9:39 AM
// * To change this template use File | Settings | File Templates.
// */
//public abstract class BdbCriteria implements Criteria {
//
//
//    @Override
//    public Criteria and(Criteria criteria) {
//        return new And(this, (BdbCriteria) criteria);
//    }
//
//    @Override
//    public Criteria or(Criteria criteria) {
//        return new Or(this, (BdbCriteria) criteria);
//    }
//
//    public abstract boolean isCriteriaFulfilledFor(Object o);
//
//    public static class And extends BdbCriteria {
//        BdbCriteria criteria1;
//        BdbCriteria criteria2;
//
//        public And(BdbCriteria criteria1, BdbCriteria criteria2) {
//            this.criteria1 = criteria1;
//            this.criteria2 = criteria2;
//        }
//
//        @Override
//        public boolean isCriteriaFulfilledFor(Object o) {
//            return criteria1.isCriteriaFulfilledFor(o) && criteria2.isCriteriaFulfilledFor(o);
//        }
//    }
//
//    public static class Or extends BdbCriteria {
//        BdbCriteria criteria1;
//        BdbCriteria criteria2;
//
//        public Or(BdbCriteria criteria1, BdbCriteria criteria2) {
//            this.criteria1 = criteria1;
//            this.criteria2 = criteria2;
//        }
//
//        @Override
//        public boolean isCriteriaFulfilledFor(Object o) {
//            return criteria1.isCriteriaFulfilledFor(o) || criteria2.isCriteriaFulfilledFor(o);
//        }
//    }
//
//
//
//
//}
