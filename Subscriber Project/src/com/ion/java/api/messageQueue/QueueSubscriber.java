package com.ion.java.api.messageQueue;

import com.ion.java.api.bean.MyPrice;
import com.iontrading.mkv.*;
import com.iontrading.mkv.enums.MkvObjectType;
import com.iontrading.mkv.enums.MkvPlatformEvent;
import com.iontrading.mkv.events.*;
import com.iontrading.mkv.exceptions.MkvConnectionException;
import com.iontrading.mkv.exceptions.MkvException;
import com.iontrading.mkv.exceptions.MkvObjectNotAvailableException;
import com.iontrading.mkv.exceptions.MkvTypeNotFoundException;
import com.iontrading.mkv.helper.MkvSubscribeProxy;
import com.iontrading.mkv.messagequeue.MkvMQSubscribe;
import com.iontrading.mkv.messagequeue.MkvMQSubscribeConf;
import com.iontrading.mkv.qos.MkvQoS;

/**
 * Created by divya.gupta on 31-05-2018.
 */

/**
 * Record listener is for listening records
 * <p>
 * Through the MkvAvailabilityListener you can listen publish events related to specific objects.
 * A specific onPublish method must be implemented in order to be notified as soon as a publish object has been received or a publish object is not available anymore.
 */
public class QueueSubscriber implements MkvPlatformListener, MkvPublishListener, MkvRecordListener, MkvAvailabilityListener, MkvMQSubscribeListener {

    private MkvSubscribeProxy subProxy = null;
    private MyPrice myPrice = null;
    static String SOURCE = "DGA_SRC1";

    @Override
    public void onMain(MkvPlatformEvent mkvPlatformEvent) {
        if (mkvPlatformEvent.equals(MkvPlatformEvent.START)) {
            Mkv.getInstance().getPublishManager().addPublishListener(this);
            AddAvailabilityListener();

        }
    }

    /**
     * We can reuse the MyPrice bean developed for the Publishing section of this tutorial, in order to managed the supplied values in a more readable way.
     * An MkvSubscribeProxy will be used to wrap the link between the MyPrice bean and the MkvType of the MkvSupply object received from the publisher.
     * The MkvSubscribeProxy will also take care of automatically map the fields of the MkvType with the ones of the MyPrice bean.
     */
    public void AddMkvSubscribeProxy() {
        subProxy = new MkvSubscribeProxy(MyPrice.class);
        myPrice = new MyPrice();
    }

    public void AddAvailabilityListener() {
        MkvPublishManager pm = Mkv.getInstance().getPublishManager();
        pm.addAvailabilityListener("USD.PRICE." + SOURCE + ".DIV123", this);
    }

    @Override
    public void onComponent(MkvComponent mkvComponent, boolean b) {

    }

    @Override
    public void onConnect(String s, boolean b) {

    }

    public static void main(String[] args) {
// create the initial configuration used to start the engine.
        MkvQoS qos = new MkvQoS();

// setting the command line arguments enriched if it is the case
        qos.setArgs(args);

// install the platform listener
        qos.setPlatformListeners(new MkvPlatformListener[]{new QueueSubscriber()});
        try {
            // Start the engine and get back the instance of
            // Mkv (unique during the life of a component).
            Mkv mkv = Mkv.start(qos);
        } catch (MkvException e) {
        }
    }


    /**
     * MkvPublishListener onPublish method
     *
     * @param mkvObject
     * @param start
     * @param dwl
     */
    @Override
    public void onPublish(MkvObject mkvObject, boolean start, boolean dwl) {
        if (mkvObject.getName().contains("USD")) {
            MkvMQSubscribe sub = Mkv.getInstance().getMQManager().createSubscription(
                    "TestQueue",
                    new QueueSubscriber(), // The class we defined above
                    new MkvMQSubscribeConf()
            );

// Create a subscription using this queue subscriber:
            sub.subscribe(new String[]{"BID", "ASK"});
        }
    }

    @Override
    public void onPublishIdle(String component, boolean start) {
        System.out.println("Publish Idle");
    }

    /**
     * There are two ways to subscribe record and chains:
     * 1) Persistent subscription (suggested approach) : it's a declarative subscription, the only constraint is that you can call it after the START event has been received.
     * With this you delegate to the API the subscription, that is the API will decide the right time to send the subscription towards to the platform.
     * 2) Manual subscription : you adopt this subscription method when you want to have the full control of when the subscription is made. As a best practice when doing manual subscription you have to:
     * a) wait for the idle, if you haven't received yet.
     * b) after you have received the idle : check if the object you want to subscribe is available into the data dictionary and subscribe it.
     * c) If you have received the idle and the object is not into the data dictionary, check each new publication and subscribe the record/chain as soon as the published object you need is available.
     * The reason for which you must not subscribe records before the idle is that typically data dictionary takes lot of space and we want that a component will quickly manage publish event coming in a row from another component.
     *
     * @param mkvObject
     */
    @Override
    public void onSubscribe(MkvObject mkvObject) {
        System.out.println("subscribing called................");
    }

    @Override
    public void onPartialUpdate(MkvRecord mkvRecord, MkvSupply mkvSupply, boolean isSnapshot) {
        System.out.println();
        System.out.println("Subscribed record onPartialUpdate:::");

        System.out.println("MKV REcord---");
        System.out.println("mkvRecord.getName():: " + mkvRecord.getName());
        System.out.println("mkvRecord.getFrom():: " + mkvRecord.getFrom());
        System.out.println("mkvRecord.getErrorMessage():: " + mkvRecord.getErrorMessage());
        System.out.println("mkvRecord.getOrig():: " + mkvRecord.getOrig());
        System.out.println("mkvRecord.getPattern():: " + mkvRecord.getPattern());
        System.out.println("mkvRecord.getPublishTimeStamp():: " + mkvRecord.getPublishTimeStamp());

    }

    /**
     * The MkvSupply object passed with onFullUpdate and onPartialUpdate is a temporary object that points to the real cache on the record. It is then reused by the API to improve performance, for example, to reduce the impact on the garbage collector.
     *
     * @param mkvRecord
     * @param mkvSupply
     * @param isSnapshot
     */
    @Override
    public void onFullUpdate(MkvRecord mkvRecord, MkvSupply mkvSupply, boolean isSnapshot) {
        System.out.println();
        System.out.println("Subscribed record onFullUpdate:::");

        System.out.println("MKV REcord---");
        System.out.println("mkvRecord.getName():: " + mkvRecord.getName());
        System.out.println("mkvRecord.getFrom():: " + mkvRecord.getFrom());
        System.out.println("mkvRecord.getErrorMessage():: " + mkvRecord.getErrorMessage());
        System.out.println("mkvRecord.getOrig():: " + mkvRecord.getOrig());
        System.out.println("mkvRecord.getPattern():: " + mkvRecord.getPattern());
        System.out.println("mkvRecord.getPublishTimeStamp():: " + mkvRecord.getPublishTimeStamp());

        try {
            subProxy.update(mkvRecord, mkvSupply, myPrice);
            System.out.println("id: " + myPrice.getId() + " bid: " + myPrice.getBid() + " pattern: " + mkvRecord.getPattern());
        } catch (MkvException e) {
            e.printStackTrace();
        }

    }

    /**
     * MkvAvailabilityListener onPublish method
     * <p>
     * <p>
     * gets called when we either connect/disconnect component from router
     *
     * @param mkvObject
     * @param b
     */
    @Override
    public void onPublish(MkvObject mkvObject, boolean b) {
        System.out.println("onPublish of Availability listener called..........");
    }


    /**
     * This is called when a record is received from the queue
     *
     * @param mkvMQSubscribeEvent
     */
    @Override
    public void onUpdate(MkvMQSubscribeEvent mkvMQSubscribeEvent) {
        System.out.println("Queue received");
        AddMkvSubscribeProxy();
        try {
            subProxy.update(mkvMQSubscribeEvent.getMessageQueueSubscribe().getMkvRecord(),  mkvMQSubscribeEvent.getSupply(), myPrice);
            System.out.println("id: " + myPrice.getId() + " bid: " + myPrice.getBid());
        } catch (MkvException e) {
            e.printStackTrace();
        } catch (Exception e){
            e.printStackTrace();
        }
    }
}
