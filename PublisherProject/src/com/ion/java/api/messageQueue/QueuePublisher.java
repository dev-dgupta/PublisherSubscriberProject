package com.ion.java.api.messageQueue;

import com.ion.java.api.bean.MyPrice;
import com.ion.java.api.publisher.MySupplyApplication;
import com.iontrading.mkv.*;
import com.iontrading.mkv.enums.MkvFieldType;
import com.iontrading.mkv.enums.MkvPlatformEvent;
import com.iontrading.mkv.events.MkvPlatformListener;
import com.iontrading.mkv.events.MkvPublishListener;
import com.iontrading.mkv.exceptions.*;
import com.iontrading.mkv.helper.MkvSupplyFactory;
import com.iontrading.mkv.helper.MkvSupplyProxy;
import com.iontrading.mkv.messagequeue.MkvMQ;
import com.iontrading.mkv.messagequeue.MkvMQConf;
import com.iontrading.mkv.qos.MkvQoS;

/**
 * Created by divya.gupta on 05-06-2018.
 */
public class QueuePublisher implements MkvPlatformListener, MkvPublishListener {

    static private MkvSupplyProxy Proxy;
    MkvRecord myRec1 = null;
    MkvRecord myRec2 = null;
    MkvRecord myRec3 = null;

    @Override
    public void onMain(MkvPlatformEvent mkvPlatformEvent) {
        switch (mkvPlatformEvent.intValue()) {
            case MkvPlatformEvent.START_code:

                //creating publish listener
                Mkv mkv = Mkv.getInstance();
                MkvPublishListener publishListener = this;
                mkv.getPublishManager().addPublishListener(publishListener);
                String SOURCE = "DGA_SRC1";
                String TYPE = "DGA_TYP3";
                // create a compliant type name
                String TYPENAME = SOURCE + "_" + TYPE;

                String[] NAMES = {"Id", "Ask", "Bid", "Qty"};
                MkvFieldType[] TYPES = {
                        MkvFieldType.STR, MkvFieldType.REAL,
                        MkvFieldType.REAL, MkvFieldType.REAL};

                // create the object passing the name and
                // the fields to the constructor
                MkvType myType = null;
                try {
                    myType = new MkvType(TYPENAME, NAMES, TYPES);
                    MkvMQConf conf = new MkvMQConf();
                    MkvMQ queue = mkv.getMQManager().create("TestQueue", myType, conf);
                    // publish the type

                    publishType(myType);
                    publishRecord();
//                    publishSupplyChain();
//                    SupplyRecordIT001(queue);
//                    SupplyRecordIT002(queue);
                    SupplyRecordIT003(queue);
                } catch (MkvException e) {
                    e.printStackTrace();
                }

                break;
            case MkvPlatformEvent.STOP_code:
                // component has stopped, prepare for graceful shutdown break;
            case MkvPlatformEvent.REGISTER_code:
                System.out.println("entered registered codee ");
                break;
            case MkvPlatformEvent.REGISTER_IDLE_code: // component can share data on the bus
                break;
        }
    }

    /**
     * A chain is an aggregation of congruently typed record names into an ordered list. A record name may belong to no chains, or one or multiple chains, it’s up to the publisher decide which record names belong to which chain.
     * <p>
     * It is possible to subscribe to a chain to discover the set of record names in the chain.
     */
    public void publishSupplyChain() {
        //defining the specs, use <your name> instead of SRC
        MkvChain chain;
        try {
            chain = new MkvChain("USD.PRICE.DGA_SRC1.DEPTH", "DGA_SRC1_DGA_TYP3");
            chain.publish();
            //append some records, using record names
            chain.add("USD.PRICE.DGA_SRC1." + "DIV123");
            chain.add("USD.PRICE.DGA_SRC1." + "DIV124");
            chain.add("USD.PRICE.DGA_SRC1." + "DIV125");
        } catch (MkvException e) {
            e.printStackTrace();
        }
    }

    private void publishType(MkvType myType) {
        System.out.println();
        System.out.println();
        System.out.println("Publishing type..........");
        try {
            myType.publish();
        } catch (MkvException e) {
            e.printStackTrace();
        }

    }

    private void publishRecord() {
        System.out.println();
        System.out.println();
        System.out.println("Publishing records..........");
//        String CURRENCY = mkvProperties.getProperty("currency");
        String CURRENCY = "USD";
//        String SOURCE = mkvProperties.getProperty("source");
        String INSTRUMENT = "PRICE";
        String SOURCE = "DGA_SRC1";
        String TYPE = "DGA_TYP3";
        // create a compliant type name
        String TYPENAME = SOURCE + "_" + TYPE;
        try {

            // publish the record
            // Object publication is done by calling MkvObject.publish() method.
//            myRec1 = new MkvRecord(CURRENCY + "." + INSTRUMENT + "." + SOURCE + ".DIV123", TYPENAME);
//            myRec2 = new MkvRecord(CURRENCY + "." + INSTRUMENT + "." + SOURCE + ".DIV124", TYPENAME);
            myRec3 = new MkvRecord(CURRENCY + "." + INSTRUMENT + "." + SOURCE + ".DIV125", TYPENAME);
//            myRec1.publish();
//            myRec2.publish();
            myRec3.publish();

        } catch (MkvException e) {
            e.printStackTrace();
        }

    }

    public void SupplyRecordIT001() {
        System.out.println();
        System.out.println("supply records DIV123.....");
        int[] fieldsIdx = {
                myRec1.getMkvType().getFieldIndex("Id"),
                myRec1.getMkvType().getFieldIndex("Bid"), // optimize caching
                myRec1.getMkvType().getFieldIndex("Ask") // indexes somewhere
        };

        Object[] values = {new String("DIV123"), new Double(99.1), new
                Double(100.1)};
        try {
            myRec1.supply(fieldsIdx, values);
        } catch (MkvObjectNotAvailableException e) {
            e.printStackTrace();
        } catch (MkvTypeNotFoundException e) {
            e.printStackTrace();
        } catch (MkvInvalidSupplyException e) {
            e.printStackTrace();
        }
    }

    public void SupplyRecordIT002() {
        System.out.println();
        System.out.println("supply records DIV124.....");
        String[] fields = {"Id", "Bid", "Ask"};
        Object[] values = {new String("DIV124"), new Double(99.1), new
                Double(100.1)};
        try {
            myRec2.supply(fields, values);
        } catch (MkvObjectNotAvailableException e) {
            e.printStackTrace();
        } catch (MkvTypeNotFoundException e) {
            e.printStackTrace();
        } catch (MkvInvalidSupplyException e) {
            e.printStackTrace();
        }
    }

    static public MkvSupplyProxy createProxy() throws Exception {
        System.out.println();
        System.out.println("proxy created.....");
        if (Proxy == null) {
            MkvPublishManager pm = Mkv.getInstance().getPublishManager();
            String SOURCE = "DGA_SRC1";
            String TYPE = "DGA_TYP3";
            // create a compliant type name
            String TYPENAME = SOURCE + "_" + TYPE;
            MkvType type = pm.getMkvType(TYPENAME);
            // class MyPrice is a bean with ID,ASK,BID,QTY
            // the create method map an ION type to a bean
            Proxy = MkvSupplyFactory.create(type, MyPrice.class);
            return Proxy;
        } else {
            return Proxy.getClass().newInstance();
        }
    }

    public void SupplyRecordIT003(MkvMQ queue) {
        System.out.println();
        System.out.println("supply records DIV125.....");
        MyPrice myPrice3 = new MyPrice();
        MkvSupplyProxy mkvSupplyProxy = null;
        try {
            mkvSupplyProxy = MySupplyApplication.createProxy();
            myPrice3.setId("DIV125");
            myPrice3.setAsk(99.8);
            myPrice3.setBid(99.9);
            mkvSupplyProxy.set(myPrice3);
            myRec3.supply(mkvSupplyProxy);
            queue.put(mkvSupplyProxy);
        } catch (MkvFieldNotFoundException e) {
            e.printStackTrace();
        } catch (MkvObjectNotLocalException e) {
            e.printStackTrace();
        } catch (MkvObjectNotAvailableException e) {
            e.printStackTrace();
        } catch (MkvTypeNotFoundException e) {
            e.printStackTrace();
        } catch (MkvInvalidSupplyException e) {
            e.printStackTrace();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public void SupplyRecordIT001(MkvMQ queue) {
        System.out.println();
        System.out.println("supply records DIV125.....");
        MyPrice myPrice3 = new MyPrice();
        MkvSupplyProxy mkvSupplyProxy = null;
        try {
            mkvSupplyProxy = MySupplyApplication.createProxy();
            myPrice3.setId("DIV123");
            myPrice3.setAsk(9.8);
            myPrice3.setBid(19.9);
            mkvSupplyProxy.set(myPrice3);
//            myRec3.supply(mkvSupplyProxy);
            queue.put(mkvSupplyProxy);
        } catch (MkvFieldNotFoundException e) {
            e.printStackTrace();
        } catch (MkvObjectNotLocalException e) {
            e.printStackTrace();
        } catch (MkvObjectNotAvailableException e) {
            e.printStackTrace();
        } catch (MkvTypeNotFoundException e) {
            e.printStackTrace();
        } catch (MkvInvalidSupplyException e) {
            e.printStackTrace();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public void SupplyRecordIT002(MkvMQ queue) {
        System.out.println();
        System.out.println("supply records DIV125.....");
        MyPrice myPrice3 = new MyPrice();
        MkvSupplyProxy mkvSupplyProxy = null;
        try {
            mkvSupplyProxy = MySupplyApplication.createProxy();
            myPrice3.setId("DIV124");
            myPrice3.setAsk(49.8);
            myPrice3.setBid(78.9);
            mkvSupplyProxy.set(myPrice3);
//            myRec3.supply(mkvSupplyProxy);
            queue.put(mkvSupplyProxy);
        } catch (MkvFieldNotFoundException e) {
            e.printStackTrace();
        } catch (MkvObjectNotLocalException e) {
            e.printStackTrace();
        } catch (MkvObjectNotAvailableException e) {
            e.printStackTrace();
        } catch (MkvTypeNotFoundException e) {
            e.printStackTrace();
        } catch (MkvInvalidSupplyException e) {
            e.printStackTrace();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Override
    public void onComponent(MkvComponent mkvComponent, boolean b) {

    }

    @Override
    public void onConnect(String s, boolean b) {

    }

    @Override
    public void onPublish(MkvObject mkvObject, boolean b, boolean b1) {

    }

    @Override
    public void onPublishIdle(String s, boolean b) {

    }

    @Override
    public void onSubscribe(MkvObject mkvObject) {

    }

    public static void main(String[] args) {
        // create the initial configuration used to start the engine.
        MkvQoS qos = new MkvQoS();
        // setting the command line arguments enriched if it is the case
        qos.setArgs(args);
        // install the platform listener
        qos.setPlatformListeners(new MkvPlatformListener[]{new QueuePublisher()});
        try {
            // Start the engine and get back the instance of
            // Mkv (unique during the life of a component).
            Mkv.start(qos);

        } catch (MkvException e) {
            // handle the exception
        }
    }
}
