package com.mt.hbase.chpt05.rowkeydesign;

public class RowKeyUtil {

    /**
     * 补齐20位，再反转
     *
     * @param userId
     * @return
     */
    public String formatUserId(long userId) {
        String str = String.format("%0" + 20 + "d", userId);
        StringBuilder sb = new StringBuilder(str);
        return sb.reverse().toString();
    }

    /**
     * Long.MAX_VALUE-timestamp 得到的值 再补齐20位
     *
     * @param timestamp, eg: "1479024369000"
     * @return
     */
    public String formatTimeStamp(long timestamp) {
        if(timestamp < 0){
            timestamp = 0;
        }
        long diff = Long.MAX_VALUE - timestamp;
        return String.format("%0" + 20 + "d", diff);
    }

    public static void main(String [] args){

        RowKeyUtil rowKeyUtil = new RowKeyUtil();
        //下行输出结果为：54321000000000000000
        System.out.println(rowKeyUtil.formatUserId(12345L));

        long time = System.currentTimeMillis();
        //运行时下行输出结果为：09223370520503124434
        System.out.println(rowKeyUtil.formatTimeStamp(time));

        System.out.println(rowKeyUtil.formatUserId(12345)+ rowKeyUtil.formatTimeStamp(System.currentTimeMillis())+1);

    }


}
