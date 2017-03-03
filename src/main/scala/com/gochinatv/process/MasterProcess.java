package com.gochinatv.process;


import java.io.BufferedReader;
import java.io.InputStreamReader;

/**
 * Created by zhuhh on 17/2/8.
 */
public class MasterProcess {

    public static void main(String[] args) {
        String line = null;
        try {
            Process process = Runtime.getRuntime().exec("java SlaveProcess");
            BufferedReader bufferedReader = new BufferedReader(
                    new InputStreamReader(process.getInputStream()));
            while ((line = bufferedReader.readLine()) != null)
                System.out.println(line);
            process.waitFor();

            Thread.sleep(1000);
            System.out.println("=============MasterProcess exit=============");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
