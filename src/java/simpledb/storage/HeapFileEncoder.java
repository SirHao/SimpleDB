package simpledb.storage;

import simpledb.common.Type;
import simpledb.common.Utility;

import java.io.*;
import java.util.Arrays;
import java.util.List;

/**
 * HeapFileEncoder reads a comma delimited text file or accepts
 * an array of tuples and converts it to
 * pages of binary data in the appropriate format for simpledb heap pages
 * Pages are padded out to a specified length, and written consecutive in a
 * data file.
 */

public class HeapFileEncoder {

  /** Convert the specified tuple list (with only integer fields) into a binary
   * page file. <br>
   *
   * The format of the output file will be as specified in HeapPage and
   * HeapFile.
   *
   * @see HeapPage
   * @see HeapFile
   * @param tuples the tuples - a list of tuples, each represented by a list of integers that are
   *        the field values for that tuple.
   * @param outFile The output file to write data to
   * @param npagebytes The number of bytes per page in the output file
   * @param numFields the number of fields in each input tuple
   * @throws IOException if the temporary/output file can't be opened
   */
  public static void convert(List<List<Integer>> tuples, File outFile, int npagebytes, int numFields) throws IOException {
      // 创建临时文件
      File tempInput = File.createTempFile("tempTable", ".txt");
      tempInput.deleteOnExit();
      // 写流
      BufferedWriter bw = new BufferedWriter(new FileWriter(tempInput));
      // 遍历元组, 统计元组数量
      for (List<Integer> tuple : tuples) {
          int writtenFields = 0;
          for (Integer field : tuple) {
              writtenFields++;
              // 如果超过每个字段的元组数，抛出异常
              if (writtenFields > numFields) {
                  throw new RuntimeException("Tuple has more than " + numFields + " fields: (" +
                          Utility.listToString(tuple) + ")");
              }
              // 写入当前元组
              bw.write(String.valueOf(field));
              // 分隔
              if (writtenFields < numFields) {
                  bw.write(',');
              }
          }
          // 换行
          bw.write('\n');
      }
      bw.close();
      // 传入下一个当作输入文件
      convert(tempInput, outFile, npagebytes, numFields);
  }

      public static void convert(File inFile, File outFile, int npagebytes,
                 int numFields) throws IOException {
      // 创建类型，每一个赋初值位 INT
      Type[] ts = new Type[numFields];
          Arrays.fill(ts, Type.INT_TYPE);
      convert(inFile,outFile,npagebytes,numFields,ts);
      }

  public static void convert(File inFile, File outFile, int npagebytes,
                 int numFields, Type[] typeAr)
      throws IOException {
      convert(inFile,outFile,npagebytes,numFields,typeAr,',');
  }

   /** Convert the specified input text file into a binary
    * page file. <br>
    * Assume format of the input file is (note that only integer fields are
    * supported):<br>
    * int,...,int\n<br>
    * int,...,int\n<br>
    * ...<br>
    * where each row represents a tuple.<br>
    * <p>
    * The format of the output file will be as specified in HeapPage and
    * HeapFile.
    *
    * @see HeapPage
    * @see HeapFile
    * @param inFile The input file to read data from
    * @param outFile The output file to write data to
    * @param npagebytes The number of bytes per page in the output file
    * @param numFields the number of fields in each input line/output tuple
    * @throws IOException if the input/output file can't be opened or a
    *   malformed input line is encountered
    */
  public static void convert(File inFile, File outFile, int npagebytes,
                 int numFields, Type[] typeAr, char fieldSeparator)
      throws IOException {
      // 统计类型的总字节数
      int nrecbytes = 0;
      for (int i = 0; i < numFields ; i++) {
          nrecbytes += typeAr[i].getLen();
      }
      // 计算每页存储的的元组数量
      int nrecords = (npagebytes * 8) /  (nrecbytes * 8 + 1);  //floor comes for free
      
    //  per record, we need one bit; there are nrecords per page, so we need
    // nrecords bits, i.e., ((nrecords/32)+1) integers.
      // 计算所需要的物理大小，一字节存储 8 位，以字节为单位，向上取整
    int nheaderbytes = (nrecords / 8);
    if (nheaderbytes * 8 < nrecords)
        nheaderbytes++;  //ceiling
      //恢复成比特数量
    int nheaderbits = nheaderbytes * 8;

    BufferedReader br = new BufferedReader(new FileReader(inFile));
    FileOutputStream os = new FileOutputStream(outFile);

    // our numbers probably won't be much larger than 1024 digits
    char[] buf = new char[1024];

    // 字符指针
    int curpos = 0;
    // 行计数（元组计数）
    int recordcount = 0;
    int npages = 0;
    // 字符计数
    int fieldNo = 0;

    ByteArrayOutputStream headerBAOS = new ByteArrayOutputStream(nheaderbytes);
    DataOutputStream headerStream = new DataOutputStream(headerBAOS);
    ByteArrayOutputStream pageBAOS = new ByteArrayOutputStream(npagebytes);
    DataOutputStream pageStream = new DataOutputStream(pageBAOS);

    boolean done = false;
    boolean first = true;
    while (!done) {
        // 读取指针所指字符
        int c = br.read();
        
        // Ignore Windows/Notepad special line endings
        if (c == '\r')
            continue;

        // 换行跳到下一行
        if (c == '\n') {
            if (first)
                continue;
            // 行数 + 1
            recordcount++;
            first = true;
        } else
            first = false;


        // 如果等于 分隔符， 换行
        if (c == fieldSeparator || c == '\n' || c == '\r') {
            // 读取当前已经写入 buf 的字符串
            String s = new String(buf, 0, curpos);
            // 判断类型
            if (typeAr[fieldNo] == Type.INT_TYPE) {
                try {
                    // 删除前导空格和后置空格 然后转整型后写入
                    pageStream.writeInt(Integer.parseInt(s.trim()));
                } catch (NumberFormatException e) {
                    System.out.println ("BAD LINE : " + s);
                }
            }
            else   if (typeAr[fieldNo] == Type.STRING_TYPE) {
                s = s.trim();
                // 计算长度, 是否大于最大限制
                int overflow = Type.STRING_LEN - s.length();
                // 大于String允许最大限制，截取
                if (overflow < 0) {
                    s  = s.substring(0,Type.STRING_LEN);
                }
                // 写入字符串长度
                pageStream.writeInt(s.length());
                // 写入字符串（有可能是截取的）
                pageStream.writeBytes(s);
                // 如果未满，填充 byte 0
                while (overflow-- > 0)
                    pageStream.write((byte)0);
            }
            // 计数重置
            curpos = 0;
            // 换行
            if (c == '\n')
                // 字符计数重置
                fieldNo = 0;
            else
                // 字符计数 + 1
                fieldNo++;

            // 结束字符
        } else if (c == -1) {
            done = true;
            
        }
        // c 为字符，记录到buff
        else {
            buf[curpos++] = (char)c;
            continue;
        }
        
        // if we wrote a full page of records, or if we're done altogether,
        // write out the header of the page.
        //
        // in the header, write a 1 for bits that correspond to records we've
        // written and 0 for empty slots.
        //
        // when we're done, also flush the page to disk, but only if it has
        // records on it.  however, if this file is empty, do flush an empty
        // page to disk.

        // 元组总数是否大于等于每页的最大元组数， 是否结束 并且 元组总数 > 0， 结束 并且 页面 = 0
        // 也就是大于等于一页，需要提前分割
        if (recordcount >= nrecords
            || done && recordcount > 0
            || done && npages == 0) {
            int i = 0;
            byte headerbyte = 0;
            // 落到相应槽位，对应点置 1
            for (i=0; i<nheaderbits; i++) {
                // 每一行入槽位
                if (i < recordcount)
                    headerbyte |= (1 << (i % 8));

                // 写入当前槽位
                if (((i+1) % 8) == 0) {
                    headerStream.writeByte(headerbyte);
                    // 重置，下一槽位
                    headerbyte = 0;
                }
            }
            // 如果还有未满槽位，写入
            if (i % 8 > 0)
                headerStream.writeByte(headerbyte);
            
            // pad the rest of the page with zeroes
            // 填充满整页
            for (i=0; i<(npagebytes - (recordcount * nrecbytes + nheaderbytes)); i++)
                pageStream.writeByte(0);
            
            // write header and body to file
            headerStream.flush();
            headerBAOS.writeTo(os);
            pageStream.flush();
            pageBAOS.writeTo(os);
            
            // reset header and body for next page
            headerBAOS = new ByteArrayOutputStream(nheaderbytes);
            headerStream = new DataOutputStream(headerBAOS);
            pageBAOS = new ByteArrayOutputStream(npagebytes);
            pageStream = new DataOutputStream(pageBAOS);

            // 重置
            recordcount = 0;
            // 页面 + 1
            npages++;
        }
    }
    br.close();
    os.close();
  }
}
