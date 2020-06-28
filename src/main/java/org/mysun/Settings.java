package org.mysun;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.aeron.CommonContext;

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import java.util.Random;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class Settings
{
    public static final String AERON_DIRECTORY_NAME = CommonContext.getAeronDirectoryName();
    public static final String SETTINGS_FILE_NAME = "subscriber-settings.json";
    public static final Path PATH_TO_SETTINGS = Paths.get(AERON_DIRECTORY_NAME).resolve(SETTINGS_FILE_NAME);

    public enum Speed {
        FAST,
        SLOW;
    }

    private String channel;

    private int streamId;
    private List<Speed> subscribersSpeed;

    private int totalSubscribersNumber;

    private int currentNumber;

    public Settings() {

    }

    public Settings(
        String channel,
        int streamId,
        List<Speed> subscribersSpeed,
        int totalSubscribersNumber) {
        this.channel = channel;
        this.streamId = streamId;
        this.subscribersSpeed = subscribersSpeed;
        this.totalSubscribersNumber = totalSubscribersNumber;
        this.currentNumber = totalSubscribersNumber;
    }

    public static Settings generateSettings() {
        final Random random = new Random(System.currentTimeMillis());
        final int totalSubscribersNumber = 2;
        final List<Speed> subscribersSpeed =
            Stream.generate(() -> Speed.FAST)
                .limit(totalSubscribersNumber)
                .collect(Collectors.toList());
        subscribersSpeed.set(random.nextInt(totalSubscribersNumber), Speed.SLOW);
        return new Settings(
            "aeron:udp?endpoint=224.1.1.1:1234",
            1,
            subscribersSpeed,
            totalSubscribersNumber);
    }

    public static Settings load() throws IOException
    {
        final File settingsFile = PATH_TO_SETTINGS.toFile();
        if (!settingsFile.exists())
        {
            throw new IllegalStateException("there is no settings file. Run the publisher first!");
        }
        return new ObjectMapper().readValue(settingsFile, Settings.class);
    }

    public void save() throws IOException
    {
        final File resultFile = PATH_TO_SETTINGS.toFile();
        resultFile.delete();
        new ObjectMapper().writeValue(resultFile, this);
    }

    public int decrementCurrentNumber() {
        return --currentNumber;
    }

    public String getChannel()
    {
        return channel;
    }

    public int getStreamId()
    {
        return streamId;
    }

    public int getTotalSubscribersNumber()
    {
        return totalSubscribersNumber;
    }

    public int getCurrentNumber()
    {
        return currentNumber;
    }

    public void setCurrentNumber(int currentNumber)
    {
        this.currentNumber = currentNumber;
    }

    public List<Speed> getSubscribersSpeed()
    {
        return subscribersSpeed;
    }

    public void setChannel(String channel)
    {
        this.channel = channel;
    }

    public void setStreamId(int streamId)
    {
        this.streamId = streamId;
    }

    public void setSubscribersSpeed(List<Speed> subscribersSpeed)
    {
        this.subscribersSpeed = subscribersSpeed;
    }

    public void setTotalSubscribersNumber(int totalSubscribersNumber)
    {
        this.totalSubscribersNumber = totalSubscribersNumber;
    }
}
