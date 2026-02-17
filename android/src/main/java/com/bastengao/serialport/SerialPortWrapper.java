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
    private static final int IDLE_TIMEOUT_MS = 50;
    private static final int READ_TIMEOUT_MS = 10;

    private SerialPort serialPort;
    private EventSender sender;
    private String path;
    private OutputStream out;
    private InputStream in;
    private Thread readThread;
    private Remover remover;

    private AtomicBoolean closed = new AtomicBoolean(false);

    public SerialPortWrapper(String path, SerialPort serialPort, final EventSender sender, Remover remover) {
        this.path = path;
        this.serialPort = serialPort;
        this.sender = sender;
        this.remover = remover;
        this.out = this.serialPort.getOutputStream();
        this.in = this.serialPort.getInputStream();

        this.readThread = new Thread(new Runnable() {
            @Override
            public void run() {
                long lastReadTime = 0;
                byte[] chunk = new byte[1024];
                ByteArrayOutputStream accumulator = new ByteArrayOutputStream();

                while (!closed.get()) {
                    try {
                        if (in == null) return;

                        boolean dataAvailable = in.available() > 0;

                        if (dataAvailable) {
                            int size = in.read(chunk);
                            if (size > 0) {
                                accumulator.write(chunk, 0, size);
                                lastReadTime = System.currentTimeMillis();
                            }
                        } else if (accumulator.size() > 0) {
                            long elapsed = System.currentTimeMillis() - lastReadTime;
                            if (elapsed >= IDLE_TIMEOUT_MS) {
                                byte[] buffer = accumulator.toByteArray();
                                accumulator.reset();

                                WritableMap event = Arguments.createMap();
                                String hex = SerialPortApiModule.bytesToHex(buffer, buffer.length);
                                event.putString("data", hex);
                                event.putString("path", path);
                                sender.sendEvent(DataReceivedEvent, event);
                            } else {
                                Thread.sleep(READ_TIMEOUT_MS);
                            }
                        } else {
                            Thread.sleep(READ_TIMEOUT_MS);
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
        });
        this.readThread.start();
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
        this.readThread.interrupt();
        try {
            this.in.close();
            this.out.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
        if (this.remover != null) {
            this.remover.remove();
        }
        Log.i("serialport", "close " + this.path);
    }
}
