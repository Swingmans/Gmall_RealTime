package com.atguigu.gmall.mock.util;

import java.util.Random;

/**
 * 随机数
 */
public class RandomNum {

    public static final int getRandInt(int fromNum, int toNum) {
        return fromNum + new Random().nextInt(toNum - fromNum + 1);
    }

}
