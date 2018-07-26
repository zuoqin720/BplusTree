package com.netease.mail.holmes.BplusTree;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;

/**
 * Created by zuoqin on 2018/7/24.
 */
public class TreeRandomAccessFile implements TreeFile {

    RandomAccessFile rf;

    public TreeRandomAccessFile(String path) throws IOException{
        File f = new File(path);
        this.rf = new RandomAccessFile(f,path);
    }

    public void seek(long index) throws IOException{
        rf.seek(index);
    }
    public void writeShort(int value) throws IOException{
        rf.writeShort(value);

    }
    public void writeLong(long value) throws IOException{
        rf.writeLong(value);
    }
    public void writeInt(int value) throws IOException{
        rf.writeInt(value);

    }
    public void write(byte[] value) throws IOException{
        rf.write(value);

    }
    public long length() throws IOException{
        return rf.length();
    }
    public void setLength(long newLength) throws IOException{
        rf.setLength(newLength);
    }
    public short readShort()throws IOException{
        return rf.readShort();
    }
    public long readLong() throws  IOException{
        return rf.readLong();
    }
    public int readInt() throws IOException{
        return rf.readInt();
    }
    public int read(byte b[]) throws IOException{
        return rf.read(b);
    }
}
