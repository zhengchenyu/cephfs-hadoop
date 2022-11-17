package org.apache.hadoop.fs.ceph;

import com.ceph.fs.CephMount;

import java.io.FileNotFoundException;
import java.io.IOException;

public class CephMain {

  // java -cp /home/ke/bin/hadoop/share/hadoop/common/lib/cephfs-hadoop-0.80.6.jar:/usr/local/share/java/libcephfs.jar org.apache.hadoop.fs.ceph.CephMain

  public static void main(String[] args) throws IOException {

    System.out.println("start...." + CephMount.class);

    String username = "admin";
    String monIp = "10.201.1.20:6789";
    String userKey = "AQDV1ipj9z5vHhAAnb8RlBFbqVpv6l6dJeovNQ==";
    String mountPath = "/";

    CephOperate cephOperate = new CephOperate(username, monIp, userKey, mountPath);

    CephMount cephMount = cephOperate.getMount();
    cephMount.mkdirs("/tmp0", 0);
    cephMount.mkdirs("/tmp1", 0);
    cephMount.mkdirs("/tmp0/tmp01", 0);
    String[] children = cephMount.listdir("/");
    for (String child : children) {
      System.out.println("child : " + child);
    }

    cephOperate.umount();

  }
}
