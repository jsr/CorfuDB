package org.corfudb.util;

import java.nio.ByteBuffer;
import java.util.Arrays;

/**
 *
 * Implementation of Checksum that allows variable length checksum values.
 *
 * Created by box on 10/30/16.
 */

public class BufferChecksum implements Checksum {
    public static int CHECKSUM_VERSION = 1;

    // Size of Checksum in bytes
    public static int CHECKSUM_SIZE = 4;
    private byte[] value;

    public BufferChecksum() {
        // Default constructor
    }

    public BufferChecksum(byte[] value){
        this.value = value;
    }

    @Override
    public byte[] getValue() {
        return value;
    }

    @Override
    public int getSize() {
        return CHECKSUM_SIZE;
    }

    @Override
    public boolean equals(Object o) {
        return (o instanceof BufferChecksum) && Arrays.equals(this.value, ((BufferChecksum) o).getValue());
    }

    @Override
    public void serialize(ByteBuffer buf) {
        buf.put(value, 0, CHECKSUM_SIZE);
    }

    @Override
    public void deserialize(ByteBuffer buf) {
        value = new byte[CHECKSUM_SIZE];

        for(int x = 0; x < CHECKSUM_SIZE; x++) {
            value[x] = buf.get();
        }
    }
}
