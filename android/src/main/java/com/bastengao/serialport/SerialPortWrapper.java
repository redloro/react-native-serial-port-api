package com.bastengao.serialport;

import android.serialport.SerialPort;
import android.util.Log;

import com.facebook.react.bridge.Arguments;
import com.facebook.react.bridge.WritableMap;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.ByteArrayOutputStream;
import java.util.concurrent.atomic.AtomicBoolean;


public class SerialPortWrapper {
    public final static String DataReceivedEvent = "dataReceived";
    private static final int DEFAULT_CHUNK_SIZE = 64;
    private static final int DEFAULT_IDLE_TIMEOUT_MS = 50;
    private static final int DEFAULT_READ_TIMEOUT_MS = 10;

    private SerialPort serialPort;
    private EventSender sender;
    private String path;
    private OutputStream out;
    private InputStream in;
    private Thread readThread;
    private Remover remover;
    private int chunkSize;
    private int timeoutMs;
    private int sleepMs;
    private Byte delimiter;

    private final AtomicBoolean closed = new AtomicBoolean(false);

    public SerialPortWrapper(String path, SerialPort serialPort, 
                            Integer chunkSize, Integer timeoutMs, Integer sleepMs, Byte delimiter, 
                            final EventSender sender, Remover remover) {
        this.path = path;
        this.serialPort = serialPort;
        this.sender = sender;
        this.remover = remover;
        this.chunkSize = (chunkSize != null && chunkSize > 0) ? chunkSize : DEFAULT_CHUNK_SIZE;
        this.timeoutMs = (timeoutMs != null && timeoutMs >= 0) ? timeoutMs : DEFAULT_IDLE_TIMEOUT_MS;
        this.sleepMs = (sleepMs != null && sleepMs >= 0) ? sleepMs : DEFAULT_READ_TIMEOUT_MS;
        this.delimiter = delimiter;
        this.out = this.serialPort.getOutputStream();
        this.in = this.serialPort.getInputStream();

        this.readThread = new Thread(new Runnable() {
            @Override
            public void run() {
                if (delimiter != null) {
                    runDelimiterBasedRead();
                } else {
                    runTimeoutBasedRead();
                }
            }
        });
        this.readThread.start();
    }

    private void runDelimiterBasedRead() {
        byte[] chunk = new byte[chunkSize];
        ByteArrayOutputStream accumulator = new ByteArrayOutputStream();

        while (!closed.get()) {
            try {
                if (in == null) return;

                int size = in.read(chunk);                    
                if (size > 0) {
                    for (int i = 0; i < size; i++) {
                        accumulator.write(chunk[i]);
                        
                        if (chunk[i] == delimiter) {
                            byte[] buffer = accumulator.toByteArray();
                            accumulator.reset();
                            sendDataEvent(buffer);
                        }
                    }
                }

            } catch (IOException e) {
                e.printStackTrace();
                return;
            }
        }
    }

    private void runTimeoutBasedRead() {
        long lastReadTime = 0;
        byte[] chunk = new byte[chunkSize];
        ByteArrayOutputStream accumulator = new ByteArrayOutputStream();

        while (!closed.get()) {
            try {
                if (in == null) return;

                boolean dataAvailable = in.available() > 0;

                if (dataAvailable) {
                    int size = in.read(chunk);
                    if (size > 0) {
                        accumulator.write(chunk, 0, size);
                        lastReadTime = System.nanoTime();
                    }
                } else if (accumulator.size() > 0) {
                    long elapsed = (System.nanoTime() - lastReadTime) / 1_000_000;
                    if (elapsed >= timeoutMs) {
                        byte[] buffer = accumulator.toByteArray();
                        accumulator.reset();
                        sendDataEvent(buffer);
                    } else {
                        Thread.sleep(sleepMs);
                    }
                } else {
                    Thread.sleep(sleepMs);
                }

            } catch (IOException e) {
                e.printStackTrace();
                return;
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return;
            }
        }
    }

    private void sendDataEvent(byte[] buffer) {
        if (sender == null) return;

        WritableMap event = Arguments.createMap();
        String hex = SerialPortApiModule.bytesToHex(buffer, buffer.length);
        event.putString("data", hex);
        event.putString("path", path);
        sender.sendEvent(DataReceivedEvent, event);
    }

    public WritableMap toJS() {
        WritableMap js = Arguments.createMap();
        js.putString("path", path);
        return js;
    }

    public void write(byte[] buffer) throws IOException {
        this.out.write(buffer);
    }

    public void close() {
        this.closed.set(true);

        try {
            if (this.in != null) {
                this.in.close();
            }
        } catch (IOException e) {}

        try {
            if (this.out != null) {
                this.out.close();
            }
        } catch (IOException e) {}

        try {
            if (this.readThread != null) {
                this.readThread.interrupt();
                this.readThread.join(500);
            }
        } catch (InterruptedException e) {}

        if (this.remover != null) {
            this.remover.remove();
        }

        Log.i("serialport", "close " + this.path);
    }
}