package org.apache.hadoop.fs.ceph;

import com.ceph.fs.CephFileExtent;
import com.ceph.fs.CephMount;
import com.ceph.fs.CephStat;

import java.io.IOException;
import java.util.Arrays;

public class CephOperate {

  private CephMount mount;
  private String username;
  private String monIp;
  private String userKey;

  public CephOperate(String username, String monIp, String userKey, String mountPath) {
    this.username = username;
    this.monIp = monIp;
    this.userKey = userKey;
    this.mount = new CephMount(username);
    this.mount.conf_set("mon_host", monIp);
    mount.conf_set("key",userKey);
    mount.mount(mountPath);
  }

  //查看目录列表
  public void listDir(String path) throws IOException {
    String[] dirs = mount.listdir(path);
    System.out.println("contents of the dir: " + Arrays.asList(dirs));
  }

  //新建目录
  public void mkDir(String path) throws IOException {
    mount.mkdirs(path,0755);//0表示十进制
  }

  //删除目录
  public void delDir(String path) throws IOException {
    mount.rmdir(path);
  }

  //重命名目录or文件
  public void renameDir(String oldName, String newName) throws IOException {
    mount.rename(oldName, newName);
  }

  //删除文件
  public void delFile(String path) throws IOException {
    mount.unlink(path);
  }

  //读文件
  public void readFile(String path) {
    System.out.println("start read file...");
    int fd = -1;
    try{
      fd = mount.open(path, CephMount.O_RDWR, 0755);
      System.out.println("file fd is : " + fd);

      byte[] buf = new byte[1024];
      long size = 10;
      long offset = 0;
      long count = 0;
      while((count = mount.read(fd, buf, size, offset)) > 0){
        for(int i = 0; i < count; i++){
          System.out.print((char)buf[i]);
        }
        offset += count;
      }

    } catch (IOException e){
      e.printStackTrace();
    } finally {
      if(fd > 0){
        mount.close(fd);
      }
    }
  }

  //复制文件
  public void copyFile(String sourceFile, String targetFile){
    System.out.println("start write file...");
    int readFD = -1, createAA = -1, writeFD = -1;
    try{
      readFD = mount.open(sourceFile, CephMount.O_RDWR, 0755);
      writeFD = mount.open(targetFile, CephMount.O_RDWR | CephMount.O_CREAT, 0644);
//                createAA = mountLucy.open("aa.txt", CephMount.O_RDWR | CephMount.O_CREAT | CephMount.O_EXCL, 0644);//若文件已有， 会异常
      System.out.println("file read fd is : " + readFD);

      byte[] buf = new byte[1024];
      long size = 10;
      long offset = 0;
      long count = 0;
      while((count = mount.read(readFD, buf, size, -1)) > 0){
        mount.write(writeFD, buf, count, -1);//-1指针跟着走，若取值count，指针不动
        System.out.println("offset: " + offset);
        offset += count;
        System.out.println("writeFD position : " + mount.lseek(writeFD, 0, CephMount.SEEK_CUR));
      }

    } catch (IOException e){
      e.printStackTrace();
    } finally {
      if(readFD > 0){
        mount.close(readFD);
      }
      if(writeFD > 0){
        mount.close(writeFD);
      }
    }
  }

  //写文件
  public void writeFileWithLseek(String path, long offset, int type){
    if(type <= 0){
      type =CephMount.SEEK_CUR;
    }
    System.out.println("start write file...");
    int writeFD = -1;
    try{
      writeFD = mount.open(path, CephMount.O_RDWR | CephMount.O_APPEND, 0644);
      long pos = mount.lseek(writeFD, offset, type);
      System.out.println("pos : " + pos);
      String msg = " asdfasdfasdf123123123 \n";
      byte[] buf = msg.getBytes();
      mount.write(writeFD, buf, buf.length, pos);

    } catch (IOException e){
      e.printStackTrace();
    } finally {
      if(writeFD > 0){
        mount.close(writeFD);
      }
    }
  }

  // 判断是目录还是文件
  public void listFileOrDir(){
    int writeFD = -1;
    try{
      String[] lucyDir = mount.listdir("/");
      for(int i = 0; i < lucyDir.length; i++){
        CephStat cephStat = new CephStat();
        mount.lstat(lucyDir[i], cephStat);
        System.out.println(lucyDir[i] + " is dir : " + cephStat.isDir()
            + " is file: " + cephStat.isFile()
            + " size: " + cephStat.size
            + " blksize: " + cephStat.blksize);//cephStat.size就是文件大小
      }

      writeFD = mount.open("lucy1.txt", CephMount.O_RDWR | CephMount.O_APPEND, 0644);
      CephFileExtent cephFileExtent = mount.get_file_extent(writeFD, 0);
      System.out.println("lucy1.txt size: " + cephFileExtent.getLength());//4M
      System.out.println("lucy1.txt stripe unit: " + mount.get_file_stripe_unit(writeFD));//4M
      long pos = mount.lseek(writeFD, 0, CephMount.SEEK_END);
      System.out.println("lucy1.txt true size: " + pos);//30Byte

    } catch (IOException e){
      e.printStackTrace();
    } finally {
      if(writeFD > 0){
        mount.close(writeFD);
      }
    }
  }

  //set current dir (work dir)
  public void setWorkDir(String path) throws IOException{
    mount.chdir(path);
  }


  //外部获取mount
  public CephMount getMount(){
    return this.mount;
  }

  //umount
  public void umount(){
    mount.unmount();
  }
}
