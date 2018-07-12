package com.rong360.binlogutil;

/**
 * @author liuchi
 * 2018/6/14
 */
public class BinlogInfo {

    private String fileName;
    private long pos;

    public BinlogInfo(String name, long position) {
        fileName = name;
        pos = position;
    }

    public boolean compareBinlog(BinlogInfo binlogInfo) {
        int fileResult = this.fileName.compareToIgnoreCase(binlogInfo.getFileName());
        return fileResult > 0 || fileResult == 0 && this.pos >= binlogInfo.getPos();
    }

    public long getPos() {
        return pos;
    }

    public String getFileName() {
        return fileName;
    }
}
