package com.ion.java.api.subscriber;

import com.iontrading.mkv.MkvComponent;
import com.iontrading.mkv.MkvObject;
import com.iontrading.mkv.MkvRecord;
import com.iontrading.mkv.MkvSupply;
import com.iontrading.mkv.enums.MkvPlatformEvent;
import com.iontrading.mkv.events.MkvAvailabilityListener;
import com.iontrading.mkv.events.MkvPlatformListener;
import com.iontrading.mkv.events.MkvPublishListener;
import com.iontrading.mkv.events.MkvRecordListener;

/**
 * Created by divya.gupta on 01-06-2018.
 */
public class MyAdvancedSubsriptionApplication implements MkvRecordListener,MkvPublishListener,MkvAvailabilityListener,MkvPlatformListener{
    @Override
    public void onPublish(MkvObject mkvObject, boolean b) {

    }

    @Override
    public void onMain(MkvPlatformEvent mkvPlatformEvent) {

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

    @Override
    public void onPartialUpdate(MkvRecord mkvRecord, MkvSupply mkvSupply, boolean b) {

    }

    @Override
    public void onFullUpdate(MkvRecord mkvRecord, MkvSupply mkvSupply, boolean b) {

    }
}
