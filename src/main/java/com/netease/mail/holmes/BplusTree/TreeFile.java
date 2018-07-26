package com.netease.mail.holmes.BplusTree;

import java.io.IOException;

/**
 * Created by zuoqin on 2018/7/24.
 */
public interface TreeFile {

    public void seek(long index) throws IOException;
    public void writeShort(int value) throws IOException;
    public void writeLong(long value) throws IOException;
    public void writeInt(int value) throws IOException;
    public void write(byte[] value) throws IOException;
    public long length()throws IOException;
    public void setLength(long value) throws IOException;
    public short readShort()throws IOException;
    public long readLong() throws  IOException;
    public int readInt() throws IOException;
    public int read(byte b[]) throws IOException;

}
