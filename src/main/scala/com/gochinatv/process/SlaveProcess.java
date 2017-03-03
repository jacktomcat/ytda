package com.gochinatv.process;

import java.util.UUID;

/**
 * Created by zhuhh on 17/2/8.
 */
public class SlaveProcess {


    public static void main(String[] args) {
        try {
            for(int i=0;i<100;i++) {
                System.out.println(UUID.randomUUID().toString());
            }
            System.out.println("===========通过MasterProcess启动Slave======");
            Thread.sleep(10000);
            System.out.println("=============SlaveProcess exit=============");

        } catch (Exception e) {
            e.printStackTrace();
        }
    }

}
