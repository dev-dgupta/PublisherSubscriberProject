package com.ion.java.api;

import com.iontrading.mkv.Mkv;
import com.iontrading.mkv.MkvComponent;
import com.iontrading.mkv.MkvProperties;
import com.iontrading.mkv.enums.MkvPlatformEvent;
import com.iontrading.mkv.events.MkvPlatformListener;
import com.iontrading.mkv.exceptions.MkvException;
import com.iontrading.mkv.qos.MkvQoS;

/**
 * Created by divya.gupta on 29-05-2018.
 */
public class MyPlatformApplication implements MkvPlatformListener {

    /**
     * @param event
     */
    public void onMain(MkvPlatformEvent event) {
        System.out.println(event.intValue());
        System.out.println(event.toString());

        switch (event.intValue()) {
            case MkvPlatformEvent.START_code:
                // API have started, init business logic
                MkvProperties propertiesManager = Mkv.getInstance().getProperties();
                // reading the configuration variable mkv.custom_string_variable
                String stringProp = propertiesManager.getProperty("custom_string_variable");

                System.out.println("stringprop::" +stringProp);
                // reading the configuration variable mkv.custom_int_variable
                int intProp = propertiesManager.getIntProperty("custom_int_variable");
                System.out.println("intprop::" +intProp);
                break;
            case MkvPlatformEvent.STOP_code:
                // component has stopped, prepare for graceful shutdown break;
            case MkvPlatformEvent.REGISTER_code:
                System.out.println("entered registered codee ");
                break;
            case MkvPlatformEvent.REGISTER_IDLE_code: // component can share data on the bus break;
        }

    }

    /**
     * @param mkvComponent
     * @param b
     */
    @Override
    public void onComponent(MkvComponent mkvComponent, boolean b) {

    }

    /**
     * @param s
     * @param b
     */
    @Override
    public void onConnect(String s, boolean b) {

    }

    public static void main(String[] args) {
        // create the initial configuration used to start the engine.
        MkvQoS qos = new MkvQoS();
        // setting the command line arguments enriched if it is the case
        qos.setArgs(args);
        // install the platform listener
        qos.setPlatformListeners(new MkvPlatformListener[]{new MyPlatformApplication()});
        try {
            // Start the engine and get back the instance of
            // Mkv (unique during the life of a component).
            Mkv mkv = Mkv.start(qos);
            MkvProperties mkvProperties = mkv.getProperties();

            System.out.println(mkvProperties.getComponentName());
            System.out.println(mkvProperties.checkDebugLevel(0));

        } catch (MkvException e) {
            // handle the exception
        }
    }
}
