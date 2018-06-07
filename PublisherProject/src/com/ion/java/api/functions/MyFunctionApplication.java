package com.ion.java.api.functions;

import com.ion.java.api.bean.MyPrice;
import com.ion.java.api.publisher.MySupplyApplication;
import com.iontrading.mkv.*;
import com.iontrading.mkv.enums.MkvFieldType;
import com.iontrading.mkv.enums.MkvPlatformEvent;
import com.iontrading.mkv.events.MkvFunctionCallEvent;
import com.iontrading.mkv.events.MkvFunctionListener;
import com.iontrading.mkv.events.MkvPlatformListener;
import com.iontrading.mkv.events.MkvPublishListener;
import com.iontrading.mkv.exceptions.*;
import com.iontrading.mkv.helper.MkvSupplyFactory;
import com.iontrading.mkv.helper.MkvSupplyProxy;
import com.iontrading.mkv.qos.MkvQoS;

/**
 * Created by divya.gupta on 01-06-2018.
 */
public class MyFunctionApplication implements MkvPlatformListener, MkvPublishListener, MkvFunctionListener {


    static private MkvSupplyProxy Proxy;
    MkvRecord myRec1 = null;
    MkvRecord myRec2 = null;
    MkvRecord myRec3 = null;

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

    /**
     * Records can be automatically added to a predefined and published pattern according to their name.
     * For example if you define a pattern called "EUR.CM_DEPTH.MAX."  on “MAX_CM_DEPTH” all the records like EUR.CM_DEPTH.MAX.* will be automatically added to the pattern as soon as they are published.
     * <p>
     * To publish a pattern, you must:
     * <p>
     * 1.     Declare the type of the records as usual.
     * 2.     Publish the pattern.
     * 3.     Publish the records, exactly as above.
     */
    public void publishPattern() {
        //defining the specs, use <your name> instead of SRC
        MkvPattern pattern;
        try {
            pattern = new MkvPattern("USD.PRICE.MAX.", "MAX_PRICE");
            pattern.publish();
        } catch (MkvException e) {
            e.printStackTrace();
        }
    }

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
                publishSupplyChain();
                publishPattern();
                publishFunction();
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

    @Override
    public void onPublish(MkvObject mkvObject, boolean pub_unpub, boolean dwl) {
        System.out.println("Publish object is available:" + pub_unpub);
    }

    @Override
    public void onPublishIdle(String s, boolean b) {
        System.out.println("Download Pub value::: " + b);
        System.out.println("Component name Idle::: " + s);
    }

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
        qos.setPlatformListeners(new MkvPlatformListener[]{new MyFunctionApplication()});
        try {
            // Start the engine and get back the instance of
            // Mkv (unique during the life of a component).
            Mkv.start(qos);

        } catch (MkvException e) {
            // handle the exception
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

    @Override
    public void onCall(MkvFunctionCallEvent mkvFunctionCallEvent) {
        MkvSupply argsWrapper = mkvFunctionCallEvent.getArgs();
        boolean condition = true;
        try {
            //apply the business logic and set condition accordingly.
            if (condition) {
                double arg0;
                arg0 = argsWrapper.getDouble(0);
                double arg1 = argsWrapper.getDouble(1);
                mkvFunctionCallEvent.setResult(MkvSupplyFactory.create((arg0 + arg1) / 2));
                // The publisher should not take any assumption on when the result is sent
                // to the component calling the function.
                // It could happen that the result is sent synchronously with the call or
                // asynchronously when the ION engine takes the control of the application.
            } else {
                mkvFunctionCallEvent.setError((byte) -1, "Error!");
            }
        } catch (MkvException e) {
            e.printStackTrace();
        }
    }

    public void publishFunction() {
        MkvFunction avg_function;
        try {
            avg_function = new MkvFunction(
// the name of the function compliant to the notation SOURCE_NAME
                    "DGA_SRC1_CalculateAverage",
                    // The return type
                    MkvFieldType.REAL,
                    // the names of the arguments
                    new String[]{"Arg1", "Arg2"},
                    // the argument types
                    new MkvFieldType[]{MkvFieldType.REAL, MkvFieldType.REAL},
                    // help
                    "Calculate the average of two real values",
                    this);

            avg_function.publish();

        } catch (MkvException e) {
            e.printStackTrace();
        } // the listener that will handle the requests.
    }
}
