package com.hbase.util;

/**
 * Created by Gopi on 04-08-2016.
 */
public class Core {

    public static void main(String[] args){
        Core c = new Core();
        c.getMedia();
    }
    public String getMedia(){




        /*//FSDataInputStream in = null;

        FileInputStream fin = null;
        BufferedReader br = null;
        BufferedWriter bw = null;
//        FileOutputStream fos = null;
        byte[] b = null;
        //File file = null;
        String sCurrentLine = "";
//        String ext = fileName.substring(fileName.lastIndexOf("."), fileName.length());

        try {
            //br = new BufferedReader(new FileReader("D:\\Pic\\ac.jpg"));

            //fos = new FileOutputStream("IMG_20150620_145339217_HDR.jpg");
            //fos.write(b);
            //File inputFile = new File("D:\\Pic\\ac.jpg");
            *//*while ((sCurrentLine = br.readLine()) != null) {
                System.out.println(sCurrentLine);
            }*//*
            //bw = new BufferedWriter(new FileWriter("F:\\Pic\\IMG_20150620_145339217_HDR.jpg"));
            //bw.write(sCurrentLine);
            //file = new File(sCurrentLine);
            *//*b = FileUtils.readFileToByteArray(inputFile);
            fin = new FileInputStream(inputFile);
            fin.read(b);*//*
            //file = new File(b.toString());

            *//*FileOutputStream fileOutputStream = new FileOutputStream(inputFile);;
            fileOutputStream.write(b);*//*
            //FileSystem hdfs = FileSystem.get(appConstant.getHadoopConfig());
            //Path fileUrl = new Path(path+fileName);
            //in = hdfs.open(fileUrl);
            //IOUtils.copyBytes(in, out, 4096, false);

           *//* while ((sCurrentLine = br.readLine()) != null) {
                System.out.println(sCurrentLine);
            }*//*
            *//*out = new BufferedOutputStream(new FileOutputStream(new File(fileName)));*//*
            *//*b = new byte[ 1024];
            int numBytes = 0;
            while ((numBytes = br.read(b)) > 0) {
                out.write(b, 0, numBytes);
            }*//*
//            File file = new File("D:\\\\Pic\\\\ac.jpg");
//            System.out.println("File = " + file.length());
            //br.close();
//            fos.close();
            //in.close();
            //out.close();
            //hdfs.close();

        }catch(Exception e){
            e.printStackTrace();
        }finally{
            //IOUtils.closeStream(in);
        }*/
       // return  sCurrentLine;
        return null;
    }

}
