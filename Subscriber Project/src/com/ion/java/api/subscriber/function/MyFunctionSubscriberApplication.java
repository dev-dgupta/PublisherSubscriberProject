package com.ion.java.api.subscriber.function;

import com.ion.java.api.bean.MyPrice;
import com.iontrading.mkv.*;
import com.iontrading.mkv.enums.MkvChainAction;
import com.iontrading.mkv.enums.MkvObjectType;
import com.iontrading.mkv.enums.MkvPlatformEvent;
import com.iontrading.mkv.events.*;
import com.iontrading.mkv.exceptions.MkvConnectionException;
import com.iontrading.mkv.exceptions.MkvException;
import com.iontrading.mkv.exceptions.MkvInvalidSupplyException;
import com.iontrading.mkv.exceptions.MkvObjectNotAvailableException;
import com.iontrading.mkv.helper.MkvSubscribeProxy;
import com.iontrading.mkv.helper.MkvSupplyFactory;
import com.iontrading.mkv.qos.MkvQoS;

import java.util.List;

/**
 * Created by divya.gupta on 01-06-2018.
 */

/**
 * Subscribes a record as soon as his name is added to the chain.
 * Unsubscribes a record as soon as his name is removed from the chain.
 */
public class MyFunctionSubscriberApplication implements MkvChainListener, MkvPlatformListener, MkvPublishListener, MkvRecordListener, MkvAvailabilityListener, MkvFunctionCallListener {

    private MkvSubscribeProxy subProxy = null;
    private MyPrice myPrice = null;
    private static final String SOURCE = "DGA_SRC1";

    /**
     * A publisher has two options to manage the records belonging to a chain:
     * Add/Remove record names from a chain according to its business logic.
     * Reset/Set the overall chain on a timer basis.
     *
     * @param mkvChain
     * @param recName
     * @param pos
     * @param mkvChainAction
     */
    public void onSupply(MkvChain mkvChain, String recName, int pos, MkvChainAction mkvChainAction) {
        System.out.println("Supplied chain " + mkvChain.getName());
        try {
            switch (mkvChainAction.intValue()) {
                case MkvChainAction.IDLE_code:
                    // The component has downloaded the current content of the chain.
                    System.out.println("Chain IDLE");
                    for (Object aMkvChain : mkvChain) {
                        String _recordName =
                                (String) aMkvChain;
                        subscribeToRecord(_recordName);
                    }
                    break;
                case MkvChainAction.RESET_code:
                    // the chain has been emptied.
                    System.out.println("Chain RESET");
                    for (Object aMkvChain : mkvChain) {
                        String _recordName =
                                (String) aMkvChain;
                        unsubscribeToRecord(_recordName);
                    }
                    break;
                case MkvChainAction.INSERT_code:
                    //Is received when the publisher insert a new record name into the chain.
                case MkvChainAction.APPEND_code:
                    // a record has been appended to the chain.
                    System.out.println("Chain APPEND : " +
                            recName);
                    subscribeToRecord(recName);
                    break;
                case MkvChainAction.DELETE_code:
                    // a record has been removed from the chain.
                    System.out.println("Chain DELETE : " +
                            recName);
                    unsubscribeToRecord(recName);
                    break;
            }
        } catch (MkvException e) {
            //handle exceptions
            e.printStackTrace();
        }
    }


    private void subscribeToRecord(String recName) throws MkvException {
        MkvRecord rec =
                Mkv.getInstance().getPublishManager().getMkvRecord(recName);
        List<MkvPattern> patterns =
                Mkv.getInstance().getPublishManager().getMkvPatterns();
        patterns.stream().filter(pattern -> "USD.PRICE.MAX.".equals(pattern.getName())).forEach(pattern -> {
            System.out.println("Pattern:" + pattern.getType());
        });
        if (rec != null) {
            System.out.println("Subscribing " + recName);
            rec.subscribe(this);
        }
    }

    private void unsubscribeToRecord(String recName) throws MkvException {
        MkvRecord rec =
                Mkv.getInstance().getPublishManager().getMkvRecord(recName);
        if (rec != null) {
            System.out.println("Unsubscribing " + recName);
            rec.unsubscribe(this);
        }
    }

    @Override
    public void onPublish(MkvObject mkvObject, boolean b) {

    }

    @Override
    public void onMain(MkvPlatformEvent mkvPlatformEvent) {
        if (mkvPlatformEvent.equals(MkvPlatformEvent.START)) {
            Mkv.getInstance().getPublishManager().addPublishListener(this);
            AddAvailabilityListener();
            AddMkvSubscribeProxy();
        }
    }

    @Override
    public void onComponent(MkvComponent mkvComponent, boolean b) {

    }

    @Override
    public void onConnect(String s, boolean b) {

    }

    @Override
    public void onPublish(MkvObject mkvObject, boolean start, boolean dwl) {
        if (mkvObject.getMkvObjectType().equals(MkvObjectType.CHAIN) && start/* && !dwl*/) {
            MkvPublishManager mgr = Mkv.getInstance().getPublishManager();
            MkvChain chain = mgr.getMkvChain("USD.PRICE.DGA_SRC1.DEPTH");
            try {
                chain.subscribe(this);
            } catch (MkvObjectNotAvailableException | MkvConnectionException e) {
                e.printStackTrace();
            }

        }

        // a new object has been published at runtime
        // check if this component is interested.
        if (mkvObject.getMkvObjectType().equals(MkvObjectType.PATTERN) && start) {
            try {

// Establish a subscription to the BID and ASK fields of
// records matching this pattern
                ((MkvPattern) mkvObject).subscribe(new String[]{"BID", "ASK"}, new MyFunctionSubscriberApplication());
            } catch (Exception e) {
//
            }
        }
    }

    @Override
    public void onPublishIdle(String component, boolean start) {
        System.out.println("On publish idle");
        MkvSupply args = MkvSupplyFactory.create(new Object[]
                {new Double(2.5), new Double(3.5)});
        MkvFunction avgFn = Mkv.getInstance().getPublishManager().
                getMkvFunction("DGA_SRC1_CalculateAverage");
        try {
            avgFn.call(args, this);
            System.out.println("Function Avg called successfully");
        } catch (MkvObjectNotAvailableException e) {
            e.printStackTrace();
        } catch (MkvInvalidSupplyException e) {
            e.printStackTrace();
        } catch (MkvConnectionException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void onSubscribe(MkvObject mkvObject) {
        System.out.println("On subscribe");
    }

    @Override
    public void onPartialUpdate(MkvRecord mkvRecord, MkvSupply mkvSupply, boolean b) {
        try {
            subProxy.update(mkvRecord, mkvSupply, myPrice);
            System.out.println("id: " + myPrice.getId() + " bid: " + myPrice.getBid());
        } catch (MkvException e) {
            e.printStackTrace();
        }


    }

    @Override
    public void onFullUpdate(MkvRecord mkvRecord, MkvSupply mkvSupply, boolean b) {
        System.out.println("On full update");
    }

    public static void main(String[] args) {
        // create the initial configuration used to start the engine.
        MkvQoS qos = new MkvQoS();

// setting the command line arguments enriched if it is the case
        qos.setArgs(args);

// install the platform listener
        qos.setPlatformListeners(new MkvPlatformListener[]{new MyFunctionSubscriberApplication()});
        try {
            // Start the engine and get back the instance of
            // Mkv (unique during the life of a component).
            Mkv.start(qos);
        } catch (MkvException e) {
        }

    }

    /**
     * We can reuse the MyPrice bean developed for the Publishing section of this tutorial, in order to managed the supplied values in a more readable way.
     * An MkvSubscribeProxy will be used to wrap the link between the MyPrice bean and the MkvType of the MkvSupply object received from the publisher.
     * The MkvSubscribeProxy will also take care of automatically map the fields of the MkvType with the ones of the MyPrice bean.
     */
    private void AddMkvSubscribeProxy() {
        subProxy = new MkvSubscribeProxy(MyPrice.class);
        myPrice = new MyPrice();
    }

    private void AddAvailabilityListener() {
        MkvPublishManager pm = Mkv.getInstance().getPublishManager();
        pm.addAvailabilityListener("USD.PRICE.DGA_SRC1.DEPTH", this);
    }

    @Override
    public void onResult(MkvFunctionCallEvent mkvFunctionCallEvent, MkvSupply mkvSupply) {
        try {
            System.out.println("Result for Avg function {" + mkvSupply.getDouble(0) + "}");
        } catch (MkvException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void onError(MkvFunctionCallEvent mkvFunctionCallEvent, byte b, String s) {
        System.out.println("Error ***");
    }
}
