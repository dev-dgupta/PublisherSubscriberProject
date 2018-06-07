package com.ion.java.api.publisher;

import com.ion.java.api.bean.MyPrice;
import com.iontrading.mkv.*;
import com.iontrading.mkv.enums.MkvFieldType;
import com.iontrading.mkv.enums.MkvPlatformEvent;
import com.iontrading.mkv.events.MkvPlatformListener;
import com.iontrading.mkv.events.MkvPublishListener;
import com.iontrading.mkv.exceptions.*;
import com.iontrading.mkv.helper.MkvSupplyFactory;
import com.iontrading.mkv.helper.MkvSupplyProxy;
import com.iontrading.mkv.qos.MkvQoS;

/**
 * Created by divya.gupta on 31-05-2018.
 */
public class MySupplyApplication implements MkvPlatformListener, MkvPublishListener {


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
                publishType();
                publishRecord();
                SupplyRecordIT001();
                SupplyRecordIT002();
                SupplyRecordIT003();
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

    @Override
    public void onComponent(MkvComponent mkvComponent, boolean b) {

    }

    @Override
    public void onConnect(String s, boolean b) {

    }

    /**
     * An onPublish message can notify that a new publication is available on the platform or that a publication is no longer available on the platform.
     * A publication event can be part of a download or not. If it is part of a download, an onPublishIdle is expected at the end of the download.
     *
     * @param mkvObject
     * @param pub_unpub
     * @param dwl
     */
    @Override
    public void onPublish(MkvObject mkvObject, boolean pub_unpub, boolean dwl) {
        //This method must be called when some object has been published

        System.out.println("Publish object is available:" + pub_unpub);

//        if (pub_unpub) {


//        }

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

    public void SupplyRecordIT003() {
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

    private void publishType() {
        System.out.println();
        System.out.println();
        System.out.println("Publishing type..........");
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
            // publish the type
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
            myRec1 = new MkvRecord(CURRENCY + "." + INSTRUMENT + "." + SOURCE + ".DIV123", TYPENAME);
            myRec2 = new MkvRecord(CURRENCY + "." + INSTRUMENT + "." + SOURCE + ".DIV124", TYPENAME);
            myRec3 = new MkvRecord(CURRENCY + "." + INSTRUMENT + "." + SOURCE + ".DIV125", TYPENAME);
            myRec1.publish();
            myRec2.publish();
            myRec3.publish();

        } catch (MkvException e) {
            e.printStackTrace();
        }

    }

    /**
     * The component has just finished downloading publications from another component.
     * An onPublishIdle event can notify that the set of publications of a component are available or that the set has been removed.
     * This event can be used to subscribe to records, chains and patterns by explicitly querying the dictionary using MkvPublishManager.
     *
     * @param s
     * @param b
     */
    @Override
    public void onPublishIdle(String s, boolean b) {
        //Happens when connection gets broken or there is nothing to send to subscriber. Download Pub value=1
        System.out.println("Download Pub value::: " + b);
        System.out.println("Component name Idle::: " + s);
    }

    /**
     * Another component is subscribing to a local publication.
     *
     * @param mkvObject
     */
    @Override
    public void onSubscribe(MkvObject mkvObject) {
        System.out.println("Subscription can start.................");
    }

    public static void main(String[] args) {
        // create the initial configuration used to start the engine.
        MkvQoS qos = new MkvQoS();
        // setting the command line arguments enriched if it is the case
        qos.setArgs(args);
        // install the platform listener
        qos.setPlatformListeners(new MkvPlatformListener[]{new MySupplyApplication()});
        try {
            // Start the engine and get back the instance of
            // Mkv (unique during the life of a component).
            Mkv.start(qos);

        } catch (MkvException e) {
            // handle the exception
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
}
