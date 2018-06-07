package com.ion.java.api.publisher;

import com.iontrading.mkv.*;
import com.iontrading.mkv.enums.MkvFieldType;
import com.iontrading.mkv.enums.MkvPlatformEvent;
import com.iontrading.mkv.events.MkvPlatformListener;
import com.iontrading.mkv.events.MkvPublishListener;
import com.iontrading.mkv.exceptions.MkvException;
import com.iontrading.mkv.qos.MkvQoS;

/**
 * Created by divya.gupta on 31-05-2018.
 */
public class MyPublishApplication implements MkvPublishListener, MkvPlatformListener {
    @Override
    public void onMain(MkvPlatformEvent mkvPlatformEvent) {
        switch (mkvPlatformEvent.intValue()) {
            case MkvPlatformEvent.START_code:
                // API have started, init business logic
//                MkvProperties propertiesManager = Mkv.getInstance().getProperties();
//                // reading the configuration variable mkv.custom_string_variable
//                String stringProp = propertiesManager.getProperty("custom_string_variable");
//                // reading the configuration variable mkv.custom_int_variable
//                int intProp = propertiesManager.getIntProperty("custom_int_variable");
//
//
//                System.out.println("custom_string_variable::" + stringProp);
//                System.out.println("custom_int_variable::" + intProp);


                //creating publish listener
                Mkv mkv = Mkv.getInstance();
                MkvPublishListener publishListener = this;
                mkv.getPublishManager().addPublishListener(publishListener);


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

        if (pub_unpub) {
            System.out.println("mkvObject.getName(): " + mkvObject.getName());      // must be unique: application's responsibility
            System.out.println("mkvObject.getFrom(): " + mkvObject.getFrom());
            System.out.println("mkvObject.getOrig(): " + mkvObject.getOrig());
            System.out.println("mkvObject.getMkvObjectType(): " + mkvObject.getMkvObjectType());
            System.out.println("mkvObject.getPublishTimeStamp(): " + mkvObject.getPublishTimeStamp());

//            // we can publish object through below method
            publishType();
            publishRecord();

        }

    }

    private void publishType() {
        System.out.println();
        System.out.println();
        System.out.println("Publishing type..........");
        String SOURCE = "DGA_SRC";
        String TYPE = "DGA_TYP";

        // create a compliant name
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
        String SOURCE = "DGA_SRC";
        String TYPE = "DGA_TYPS";
        String INSTRUMENT = "PRICE";

        // create a compliant type name
        String TYPENAME = SOURCE + "_" + TYPE;

        // create a compliant record name
//        String RECORDNAME = CURRENCY + "." + INSTRUMENT + "." + SOURCE + "." + ID;

        // create the object passing the record name and the type name
        // to the constructor
        MkvRecord myRec1 = null;
        MkvRecord myRec2 = null;
        MkvRecord myRec3 = null;
        try {
            // publish the record
            // Object publication is done by calling MkvObject.publish() method.
            myRec1 = new MkvRecord( CURRENCY + "." + INSTRUMENT + "." + SOURCE + ".IT001", TYPENAME);
            myRec1.publish();
            myRec2 = new MkvRecord( CURRENCY + "." + INSTRUMENT + "." + SOURCE + ".IT001", TYPENAME);
            myRec2.publish();
            myRec3 = new MkvRecord( CURRENCY + "." + INSTRUMENT + "." + SOURCE + ".IT001", TYPENAME);
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
     * @param start
     * @param component
     */
    @Override
    public void onPublishIdle(String component, boolean start) {
        //Happens when connection gets broken or there is nothing to send to subscriber. Download Pub value=1
        System.out.println("Download Pub value::: " + start);
        System.out.println("String s of publish idle::: " + component);
    }

    /**
     * Another component is subscribing to a local publication.
     *
     * @param mkvObject
     */
    @Override
    public void onSubscribe(MkvObject mkvObject) {

    }

    public static void main(String[] args) {
        // create the initial configuration used to start the engine.
        MkvQoS qos = new MkvQoS();
        // setting the command line arguments enriched if it is the case
        qos.setArgs(args);
        // install the platform listener
        qos.setPlatformListeners(new MkvPlatformListener[]{new MyPublishApplication()});
        try {
            // Start the engine and get back the instance of
            // Mkv (unique during the life of a component).
            Mkv.start(qos);

        } catch (MkvException e) {
            // handle the exception
        }
    }
}
