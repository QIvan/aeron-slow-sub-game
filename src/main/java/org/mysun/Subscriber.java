package org.mysun;

import static org.mysun.Settings.AERON_DIRECTORY_NAME;

import io.aeron.Aeron;
import io.aeron.ChannelUri;
import io.aeron.CommonContext;
import io.aeron.Subscription;
import io.aeron.logbuffer.FragmentHandler;
import org.agrona.collections.MutableLong;
import org.agrona.concurrent.IdleStrategy;
import org.agrona.concurrent.SigInt;
import org.agrona.concurrent.SleepingIdleStrategy;
import org.agrona.concurrent.SleepingMillisIdleStrategy;

import java.util.Scanner;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

public class Subscriber implements Runnable
{
    private final Subscription subscription;
    private final AtomicBoolean isRunning;
    private final Settings.Speed mySpeed;

    public Subscriber(Subscription subscription, Settings.Speed mySpeed, AtomicBoolean isRunning)
    {
        this.subscription = subscription;
        this.mySpeed = mySpeed;
        this.isRunning = isRunning;

    }


    @Override
    public void run()
    {
        final IdleStrategy idleStrategy = new SleepingIdleStrategy(
            mySpeed == Settings.Speed.SLOW ? 100 : TimeUnit.MILLISECONDS.toNanos(100));

        final MutableLong time = new MutableLong(-1);
        final FragmentHandler fragmentHandler = (buf, offset, length, header) -> time.set(buf.getLong(offset));

        while (isRunning.get())
        {
            time.set(-1);
            while (time.get() == -1)
            {
                subscription.poll(fragmentHandler, 16);
            }
            idleStrategy.idle();
//            System.out.println("time " + time.get());
        }
    }

    public static void main(String[] args) throws Exception
    {
        final Settings settings = Settings.load();
        final int currentNumber = settings.decrementCurrentNumber();
        if (currentNumber < 0)
        {
            System.err.println("No more subscribers!");
            System.exit(1);
        }
        settings.save();
        Settings.Speed mySpeed = settings.getSubscribersSpeed().get(settings.getCurrentNumber());
        System.out.println("I'm " + mySpeed + " subscriber #" + settings.getCurrentNumber());

        final AtomicBoolean isRunning = new AtomicBoolean(true);
        SigInt.register(() -> isRunning.set(false));

        System.out.println("Connect to the Aeron...");
        try (
            final Aeron aeron = Aeron.connect(new Aeron.Context().aeronDirectoryName(Settings.AERON_DIRECTORY_NAME));
            final Subscription subscription = aeron.addSubscription(
                createChannel(settings.getChannel(), currentNumber),
                settings.getStreamId());)
        {
            final CompletableFuture<Void> subscriberTask = CompletableFuture.runAsync(
                new Subscriber(subscription, mySpeed, isRunning)
            );

            System.out.println("Connected!");

            System.out.format("Am I the slow subscriber? (y/n)").flush();
            Scanner in = new Scanner(System.in);
            String line = in.nextLine();
            final boolean answer = "y".equalsIgnoreCase(line);
            if (answer == (mySpeed == Settings.Speed.SLOW))
            {
                System.out.println("You won!");
            }
            else
            {
                System.out.println("Nooooo!!!");
                System.out.println("My number is " + currentNumber);
                System.out.println("The actual list of subscribers is " + settings.getSubscribersSpeed());

            }
            isRunning.set(false);
            subscriberTask.get();
        }
    }

    private static String createChannel(String channel, int currentNumber)
    {
        ChannelUri uri = ChannelUri.parse(channel);
        uri.put(CommonContext.ALIAS_PARAM_NAME, String.valueOf(currentNumber));
        return uri.toString();
    }

}
