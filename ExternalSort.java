package uk.ac.cam.dsjw2.fjava.tick0;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.io.DataOutputStream;
import java.io.BufferedOutputStream;
import java.io.FileOutputStream;
import java.io.DataInputStream;
import java.io.BufferedInputStream;
import java.security.DigestInputStream;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.concurrent.ThreadLocalRandom;

public class ExternalSort {
	protected static boolean isEvenArr(long num, long k) {
		return ((num%(k))==0); //check if num can be equally divided
	}
	protected static void combineTwoBlock (DataInputStream da1, DataInputStream da2,
			DataOutputStream db, long amount1, long amount2) 
					throws IOException {
		/*IMPT: set da1, da2, db pointing correctly before using
		 * Combines 2 blocks, assume that da1, da2, db are already pointing at correct start
		 * eat1 and eat2 are how much should be read in from each stream
		 * to form a sorted block of 2k in db
		 */
		//check again if holder is correct..
		//Make sure only call read if going to write
		long eat1 = amount1-1; //boundary for consuming block 1
		long eat2 = amount2-1; 
		int holderA1=0, holderA2=0; //temporary holders for each int
		int start1=0, start2=0; //point in block that stream points at
		boolean reada1 = true;
		boolean reada2 = true;

		while (start1<=eat1 || start2<=eat2) {

			//if both hit end at same time
			if (start1==eat1 && start2==eat2) {
				if (reada1) holderA1 = da1.readInt();
				if (reada2) holderA2 = da2.readInt();

				if (holderA1<=holderA2) {
					db.writeInt(holderA1);
					db.writeInt(holderA2);
					break;
				} else {
					db.writeInt(holderA2);
					db.writeInt(holderA1);
					break;
				}
			}
			//if any single block ends first
			if (start2>eat2) {
				if (reada1) {
					holderA1 = da1.readInt();
				}
				db.writeInt(holderA1);
				reada1=true;
				start1++;
				continue;
			} else if (start1>eat1) {
				if (reada2) holderA2 = da2.readInt();
				db.writeInt(holderA2);
				reada2=true;
				start2++;
				continue;
			}
			
			//normal case
			if (reada1) {
				holderA1 = da1.readInt();
				reada1=false;
			}
			if(reada2) {
				holderA2 = da2.readInt();
				reada2 = false;
			}
			if (holderA1<=holderA2) {
				db.writeInt(holderA1);
				//only read again if theres more to read
				reada1=true;
				start1++;
			} else /*if (holderA1>holderA2) */{
				db.writeInt(holderA2);
				reada2=true;
				start2++;
			}
		}
	}
	
	protected static long merge(RandomAccessFile a1, RandomAccessFile a2, 
			RandomAccessFile b, int k, long lastBlock) throws IOException {
		/*send file a to b, placing them in sorted blocks of 2k
		*return any odd remainders but ensure they are sorted first
		*/
		a1.seek(0);
		a2.seek(0);// int use k and fileLen, bytes use kInBytes and a.length
		b.seek(0); 
		//set up data stream at beginning of files
		DataInputStream da1 = new DataInputStream(
				new BufferedInputStream(new FileInputStream(a1.getFD())));
		DataInputStream da2 = new DataInputStream(
				new BufferedInputStream(new FileInputStream(a2.getFD())));
		DataOutputStream db = new DataOutputStream(
				new BufferedOutputStream(new FileOutputStream(b.getFD())));
		
		int kInBytes = k*4; //k is size of blocks
		long loopBound; //boundary for loop
		long fileLen = a1.length()/4; //number of integers in file
		long remainderBlock = (fileLen)%(2*k); 
		//number of integers in a remainder block after dividing by k

		//adjust loop bound accordingly
		if (0==remainderBlock) { //Everyone has a pair
			loopBound = fileLen; 
		} else { //no partner and doesn't fit in - leave out the misfit
			loopBound = fileLen - remainderBlock ; 
		}
		
		//merge the bulk of the blocks
		int i=0;
		boolean runFirstTime = true;
		while (i<loopBound) {
			if (!runFirstTime) {
				da1.skipBytes(kInBytes);
				da2.skipBytes(kInBytes);
			} else if (runFirstTime) {
				da2.skipBytes(kInBytes);
				runFirstTime=false;
			}
			combineTwoBlock (da1,da2,db, k, k); 
			i=i+k+k;
		}
		
		//deal with remainder/uneven cases by sorting and returning last block 
		if (0!=remainderBlock) {
			//ensure the remainder block is properly sorted
			if (0!=loopBound) da1.skipBytes(kInBytes); //if loopBound=0, both point to beginning
			da2.skipBytes((int)(remainderBlock-lastBlock)*4);//watch for precision
			combineTwoBlock(da1,da2,db,remainderBlock-lastBlock,lastBlock);	
			db.flush();
			return remainderBlock;
		}
		db.flush();
		return 0;
	}
	
	protected static void printFile(RandomAccessFile file) throws IOException {
		file.seek(0);
		DataInputStream da1 = new DataInputStream(
				new BufferedInputStream(new FileInputStream(file.getFD())));
		for (int i=0; i<(file.length()/4); i++) {
			System.out.print(Integer.toString(da1.readInt())+' ');
		}
		System.out.println();
	}
	protected static void initializefile(RandomAccessFile file) throws IOException {
		file.seek(0);
		DataOutputStream da1 = new DataOutputStream(
				new BufferedOutputStream(new FileOutputStream(file.getFD())));
		for (int i=0; i<23; i++) {
			da1.writeInt(ThreadLocalRandom.current().nextInt(1, 50 + 1));
		}
		da1.flush();
	}
	protected static void copyOver(RandomAccessFile first,RandomAccessFile second) throws IOException {
		//copy from first file to second file
		first.seek(0);
		second.seek(0);
		DataInputStream dFirst = new DataInputStream(
				new BufferedInputStream(new FileInputStream(first.getFD())));
		DataOutputStream dSecond = new DataOutputStream(
				new BufferedOutputStream(new FileOutputStream(second.getFD())));

		for (int i=0; i<(first.length()/4);i++) {
			dSecond.writeInt(dFirst.readInt());
		}
		dSecond.flush();
	}

	public static void sort(String f1, String f2) throws FileNotFoundException, IOException {
		//Prepare files for RW & create streams to buffer
		RandomAccessFile a1 = new RandomAccessFile(f1,"rw");  
		RandomAccessFile a2 = new RandomAccessFile(f1,"rw");  
		RandomAccessFile b1 = new RandomAccessFile(f2,"rw");  
		RandomAccessFile b2 = new RandomAccessFile(f2,"rw");  

		long lastBlock = 0; //for holding remainders
		boolean inA = true;
		int k=1; //block size before merge
		//initializefile(a1);
		//printFile(a1);
		//After each loop, swap files with sorted k growing each time
		while (k < (a1.length()/4)) {
			if (inA) {
				lastBlock = merge(a1,a2, b1, k, lastBlock);
				inA = false;
			} else {
				lastBlock = merge(b1,b2, a1, k, lastBlock);
				inA=true;
			}
			k*=2;
		}

		//ensure results end up in file a
		if (!inA) {copyOver(b1,a1); }
		
		//printFile(a1);
		a1.close();
		a2.close();
		b1.close();
		b2.close();
	}

	private static String byteToHex(byte b) {
		String r = Integer.toHexString(b);
		if (r.length() == 8) {
			return r.substring(6);
		}
		return r;
	}

	public static String checkSum(String f) {
		try {
			MessageDigest md = MessageDigest.getInstance("MD5");
			DigestInputStream ds = new DigestInputStream(
					new FileInputStream(f), md);
			byte[] b = new byte[512];
			while (ds.read(b) != -1)
				;

			String computed = "";
			for(byte v : md.digest()) 
				computed += byteToHex(v);
			
			ds.close();
			return computed;
		} catch (NoSuchAlgorithmException e) {
			e.printStackTrace();
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		} 
		return "<error computing checksum>";
	}

	public static void main(String[] args) throws Exception {
		String f1 = args[0];
		String f2 = args[1];
		sort(f1, f2);
		System.out.println("The checksum is: "+checkSum(f1));
		//System.out.println("The checksum is: c2cb56f4c5bf656faca0986e7eba384");
		
		//C:/Users/Joshua/Desktop/Comsci/New folder/Summer16/test-suite/test1a.dat
		//C:/Users/Joshua/Desktop/Comsci/New folder/Summer16/test-suite/test1b.dat
		
		//sort("C:/Users/Joshua/Desktop/Comsci/New folder/Summer16/example.txt",
		//		"C:/Users/Joshua/Desktop/Comsci/New folder/Summer16/blubber.txt");
		
	}

}
