/*
Copyright (c) Microsoft Corporation

All rights reserved.

Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in 
compliance with the License.  You may obtain a copy of the License 
at http://www.apache.org/licenses/LICENSE-2.0   


THIS CODE IS PROVIDED *AS IS* BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, EITHER 
EXPRESS OR IMPLIED, INCLUDING WITHOUT LIMITATION ANY IMPLIED WARRANTIES OR CONDITIONS OF 
TITLE, FITNESS FOR A PARTICULAR PURPOSE, MERCHANTABLITY OR NON-INFRINGEMENT.  


See the Apache Version 2.0 License for specific language governing permissions and 
limitations under the License. 

 */


package com.microsoft.research.peloponnese;

import java.io.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.ContentSummary;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;

//--------------------------------------------------------------------------------

public class HdfsBridge 
{
	//--------------------------------------------------------------------------------

	public static void main(String[] args) throws IOException 
	{
		Instance i = new Instance();
		int ret = i.Connect("svc-d1-17:9000");
		if (ret != SUCCESS)
		{
			System.out.println("Failed to connect");
			return;
		}

		String fileName = "/data/inputPart0.txt";

		FileStatus fs = i.OpenFileStatus(fileName);
		System.out.println(fs.getPath().toUri().getPath());

		int rc = i.IsFileExist(fileName);
		System.out.println(rc);

		if (rc == 1)
		{
			Instance.Reader r = i.OpenReader(fileName);

			if (r != null)
			{
				int nRead = 0;
				long offset = 0;
				do
				{
					Instance.Reader.Block b = r.ReadBlock(offset, 64 * 1024);
					nRead = b.ret;
					if (nRead != -1)
					{
						System.out.println("Read " + nRead + " bytes at " + offset);
						offset += nRead;
					}
				} while (nRead >= 0);

				ret = r.Close();
				if (ret != SUCCESS)
				{
					System.out.println("Failed to close");
				}
			}
		}

		ret = i.Disconnect();
		if (ret != SUCCESS)
		{
			System.out.println("Failed to disconnect");
		}
		//String fileToRead = "/data/tpch/customer_1G_128MB.txt";
		//String content = HdfsBridge.ReadBlock(fileToRead, 0);
		//System.out.println(rc);
	}

	//--------------------------------------------------------------------------------

	public static int SUCCESS = 1;
	public static int FAILURE = 0;

	//--------------------------------------------------------------------------------


	//--------------------------------------------------------------------------------

	public static class Instance
	{
		private FileSystem dfs = null;
		public String exceptionMessage = null;

		//--------------------------------------------------------------------------------

		public static class Reader
		{                       
			public static class Block
			{
				public int     ret;
				public byte[]  buffer;
			}

			private FSDataInputStream dis = null;
			public String exceptionMessage = null;

			public void Open(FileSystem dfs, String fileName) throws IOException
			{
				Path path = new Path(fileName);
				dis = dfs.open(path);
			}

			public Block ReadBlock(long blockOffset, int bytesRequested)
			{
				Block block = new Block();
				block.buffer = null;

				if (dis == null)
				{
					exceptionMessage = "ReadBlock called on closed reader";
					block.ret = -2;
					return block;
				}

				block.buffer = new byte[bytesRequested];

				int numBytesRead = -2;
				try
				{
					numBytesRead = dis.read(blockOffset, block.buffer, 0, bytesRequested); 
				}
				catch (IOException e1)
				{
					exceptionMessage = e1.getMessage();
					block.buffer = null;
					block.ret = -2;
					return block;
				}

				block.ret = numBytesRead;
				return block; 
			}

			public int Close()
			{
				int ret = SUCCESS;

				if (dis != null)
				{
					try {
						dis.close();
					}
					catch (IOException e1)
					{
						exceptionMessage = e1.getMessage();
						ret = FAILURE;
					}

					dis = null;
				}

				return ret;
			}
		}

		public static class Writer
		{
			private FileSystem dfs = null;
			private Path path = null;
			private FSDataOutputStream dos = null;
			public String exceptionMessage = null;

			public void Create(FileSystem fs, String fileName, int bufferSize, long blockSize) throws IOException
			{
				dfs = fs;
				path = new Path(fileName);

				if (blockSize < 0)
				{
					blockSize = dfs.getDefaultBlockSize(path);
				}
				dos = dfs.create(path, false, bufferSize, dfs.getDefaultReplication(path), blockSize);
			}

			public void Append(FileSystem fs, String fileName) throws IOException
			{
				dfs = fs;
				path = new Path(fileName);
				dos = dfs.append(path);
			}

			public int WriteBlock(byte[] buffer, boolean flushAfter)
			{
				if (dos == null)
				{
					exceptionMessage = "WriteBlock called on closed writer";
					return FAILURE;
				}

				try
				{
					dos.write(buffer);
					if (flushAfter)
					{
						dos.flush();
					}
				}
				catch (IOException e1)
				{
					exceptionMessage = e1.getMessage();
					return FAILURE;
				}

				return SUCCESS; 
			}
			
			public int Sync()
			{
				int ret = SUCCESS;

				try
				{
					dos.close();
					dos = dfs.append(path);
				}
				catch (IOException e1)
				{
					exceptionMessage = e1.getMessage();
					ret = FAILURE;
				}

				return ret;
			}

			public int Close()
			{
				int ret = SUCCESS;

				if (dos != null)
				{
					try
					{
						dos.close();
					}
					catch (IOException e1)
					{
						exceptionMessage = e1.getMessage();
						ret = FAILURE;
					}

					dos = null;
				}

				return ret;
			}
		}

		public static class BlockLocations
		{
			private BlockLocation[] bls = null;
			private int[] fileIndex = null;
			private String[] fileName = null;
			public String exceptionMessage = null;
			public long fileSize = -1;

			BlockLocations(
					BlockLocation[] b,
					int[] fIndex,
					String[] fName,
					long fSize)
					{
				bls = b;
				fileIndex = fIndex;
				fileName = fName;
				fileSize = fSize;
					}

			public int GetNumberOfFileNames()
			{
				return fileName.length;
			}

			public String[] GetFileNames()
			{
				return fileName;
			}

			public int GetNumberOfBlocks()
			{
				return bls.length;
			}

			public long GetBlockOffset(int blockId)
			{
				return bls[blockId].getOffset();
			}

			public long GetBlockLength(int blockId)
			{
				return bls[blockId].getLength();
			}

			public String[] GetBlockHosts(int blockId)
			{
				BlockLocation bl = bls[blockId];
				String[] hosts = null;

				exceptionMessage = null;
				try
				{
					hosts = bl.getHosts();
				}
				catch (IOException e1)
				{
					exceptionMessage = e1.getMessage();
					return null;
				}
				return hosts;
			}

			public String[] GetBlockNames(int blockId)
			{
				BlockLocation bl = bls[blockId]; 
				String[] names = null;

				exceptionMessage = null;
				try
				{
					names = bl.getNames();
				}
				catch (IOException e1)
				{
					exceptionMessage = e1.getMessage();
					return null;
				}
				return names;
			}

			public String[] GetBlockEndpoints(int blockId)
			{
				BlockLocation bl = bls[blockId];
				String[] endpoints = null;

				exceptionMessage = null;
				try
				{
					endpoints = bl.getNames();
				}
				catch (IOException e1)
				{
					exceptionMessage = e1.getMessage();
					return null;
				}
				return endpoints;
			}

			public int GetBlockFileId(int blockId)
			{
				return fileIndex[blockId];
			}
		}

		public int Connect(String schemeAndAuthority)
		{                 
			Configuration config = new Configuration();  

			if (schemeAndAuthority != null)
			{
				config.set("fs.defaultFS", schemeAndAuthority);
			}

			exceptionMessage = null;

			try                         
			{                   
				dfs = FileSystem.get(config);
			}                   
			catch (Exception e1)                      
			{
				exceptionMessage = e1.getMessage();
				return FAILURE;
			}

			return SUCCESS;
		}

		//--------------------------------------------------------------------------------

		public int Disconnect()
		{
			int ret = SUCCESS;

			if (dfs != null)
			{
				exceptionMessage = null;

				try
				{
					dfs.close();
				}
				catch (IOException e1)
				{
					exceptionMessage = e1.getMessage();
					ret = FAILURE;
				}

				dfs = null;
			}

			return ret;
		}

		//--------------------------------------------------------------------------------

		public int IsFileExist(String fileName)
		{               
			if (dfs == null)
			{
				exceptionMessage = "IsFileExist called on disconnected instance";
				return -1;
			}

			exceptionMessage = null;

			try
			{
				Path path = new Path(fileName);
				return (dfs.exists(path)) ? 1 : 0;
			}
			catch (IOException e1)
			{
				exceptionMessage = e1.getMessage();
				return -1;
			}
		}
		
		public int SetOwnerAndPermission(String pathString, String userName, String groupName, short permission)
		{
			if (dfs == null)
			{
				exceptionMessage = "SetOwnerAndPermission called on disconnected instance";
				return 0;
			}

			exceptionMessage = null;

			try
			{
				Path path = new Path(pathString);
				FsPermission permissionObject = new FsPermission(permission);
				dfs.setPermission(path, permissionObject);
				dfs.setOwner(path, userName, groupName);
				return 1;
			}
			catch (IOException e1)
			{
				exceptionMessage = e1.getMessage();
				return 0;
			}
		}

		public int EnsureDirectory(String pathString)
		{               
			if (dfs == null)
			{
				exceptionMessage = "EnsureDirectory called on disconnected instance";
				return 0;
			}

			exceptionMessage = null;

			try
			{
				Path path = new Path(pathString);
				return (dfs.mkdirs(path)) ? 1 : 0;
			}
			catch (IOException e1)
			{
				exceptionMessage = e1.getMessage();
				return 0;
			}
		}

		public int DeleteFile(String fileName, boolean recursive)
		{               
			if (dfs == null)
			{
				exceptionMessage = "DeleteFile called on disconnected instance";
				return -1;
			}

			exceptionMessage = null;

			try
			{
				Path path = new Path(fileName);
				return (dfs.delete(path, recursive)) ? 1 : 0;
			}
			catch (IOException e1)
			{
				exceptionMessage = e1.getMessage();
				return -1;
			}
		}

		public int RenameFile(String dstFileName, String srcFileName)
		{               
			if (dfs == null)
			{
				exceptionMessage = "RenameFile called on disconnected instance";
				return -1;
			}

			exceptionMessage = null;

			try
			{
				Path dstPath = new Path(dstFileName);
				Path srcPath = new Path(srcFileName);
				return (dfs.rename(srcPath, dstPath)) ? 1 : 0;
			}
			catch (IOException e1)
			{
				exceptionMessage = e1.getMessage();
				return -1;
			}
		}

		public Reader OpenReader(String fileName)
		{
			if (dfs == null)
			{
				System.out.println("OpenReader called on disconnected instance\n");
				return null;
			}

			Reader r = new Reader();

			exceptionMessage = null;

			try
			{
				r.Open(dfs, fileName);
			}
			catch (IOException e1)
			{
				exceptionMessage = e1.getMessage();
				return null;
			}

			return r;
		}

		public Writer OpenCreate(String fileName, int bufferSize, long blockSize)
		{
			if (dfs == null)
			{
				System.out.println("OpenWriter called on disconnected instance\n");
				return null;
			}

			Writer w = new Writer();

			exceptionMessage = null;

			try
			{
				w.Create(dfs, fileName, bufferSize, blockSize);
			}
			catch (IOException e1)
			{
				exceptionMessage = e1.getMessage();
				return null;
			}

			return w;
		}

		public Writer OpenAppend(String fileName)
		{
			if (dfs == null)
			{
				System.out.println("OpenWriter called on disconnected instance\n");
				return null;
			}

			Writer w = new Writer();

			exceptionMessage = null;

			try
			{
				w.Append(dfs, fileName);
			}
			catch (IOException e1)
			{
				exceptionMessage = e1.getMessage();
				return null;
			}

			return w;
		}

		public FileStatus OpenFileStatus(String fileOrDirectoryName)
		{
			if (dfs == null)
			{
				exceptionMessage = "OpenFileStatus called on disconnected instance";
				return null;
			}

			exceptionMessage = null;

			try
			{
				Path path = new Path(fileOrDirectoryName);

				return dfs.getFileStatus(path);
			}
			catch (IOException e1)
			{
				System.err.println("Exception " + e1.toString());
				exceptionMessage = e1.getMessage();
				return null;
			}
		}

		public ContentSummary OpenContentSummary(String fileOrDirectoryName)
		{
			if (dfs == null)
			{
				exceptionMessage = "OpenContentSummary called on disconnected instance";
				return null;
			}

			exceptionMessage = null;

			try
			{
				Path path = new Path(fileOrDirectoryName);

				return dfs.getContentSummary(path);
			}
			catch (IOException e1)
			{
				System.err.println("Exception " + e1.toString());
				exceptionMessage = e1.getMessage();
				return null;
			}
		}

		public BlockLocations OpenBlockLocations(FileStatus fileStatus, boolean getBlocks)
		{
			exceptionMessage = null;
			try
			{
				FileStatus[] expanded;
				if (fileStatus.isDirectory())
				{
					expanded = dfs.listStatus(fileStatus.getPath());
					for (int i=0; i<expanded.length; ++i)
					{
						if (expanded[i].isDirectory())
						{
							exceptionMessage = expanded[i].getPath().toString() + " is a directory: recursive descent not supported";
							return null;
						}
					}
				}
				else
				{
					expanded = new FileStatus[1];
					expanded[0] = fileStatus;
				}

				String[] fileName = new String[expanded.length];
				long totalSize = 0;
				for (int i=0; i<expanded.length; ++i)
				{
					fileName[i] = expanded[i].getPath().toUri().getPath();
				}

				BlockLocation[] bls = null;
				int[] fileIndex = null;

				if (getBlocks)
				{
					BlockLocation[][] nested = new BlockLocation[expanded.length][];
					int totalBlocks = 0;
					for (int i=0; i<expanded.length; ++i)
					{
						long fileLength = expanded[i].getLen();
						totalSize += fileLength;

						nested[i] = dfs.getFileBlockLocations(expanded[i], 0, fileLength);
						totalBlocks += nested[i].length;
					}

					bls = new BlockLocation[totalBlocks];
					fileIndex = new int[totalBlocks];

					int copiedBlock = 0;
					for (int i=0; i<expanded.length; ++i)
					{
						for (int j=0; j<nested[i].length; ++j, ++copiedBlock)
						{
							fileIndex[copiedBlock] = i;
							bls[copiedBlock] = nested[i][j];
						}
					}
				}

				return new BlockLocations(bls, fileIndex, fileName, totalSize);
			}
			catch (IOException e1)
			{
				exceptionMessage = e1.getMessage();
				return null;
			}
		}

		public String[] EnumerateSubdirectories(String directory)
		{
			exceptionMessage = null;
			try
			{
				Path path = new Path(directory);
				FileStatus[] contents = dfs.listStatus(path);
				int numberOfDirectories = 0;
				for (int i=0; i<contents.length; ++i)
				{
					if (contents[i].isDirectory())
					{
						++numberOfDirectories;
					}
				}

				String[] subDirectories = new String[numberOfDirectories];
				numberOfDirectories = 0;
				for (int i=0; i<contents.length; ++i)
				{
					if (contents[i].isDirectory())
					{
						subDirectories[numberOfDirectories] = contents[i].getPath().toUri().getPath();
						++numberOfDirectories;
					}
				}

				return subDirectories;
			}
			catch (IOException e1)
			{
				exceptionMessage = e1.getMessage();
				return null;
			}
		}
	}

	public static Instance OpenInstance(String schemeAndAuthority)
	{
		Instance i = new Instance();

		int ret = i.Connect(schemeAndAuthority);
		if (ret == SUCCESS)
		{
			return i;
		}
		else
		{
			System.out.println("OpenInstance failed for " + schemeAndAuthority + " -- " + i.exceptionMessage);
			return null;
		}
	}

	public static Instance OpenInstance()
	{
		Instance i = new Instance();

		int ret = i.Connect(null);
		if (ret == SUCCESS)
		{
			return i;
		}
		else
		{
			System.out.println("OpenInstance failed for default dfs -- " + i.exceptionMessage);
			return null;
		}
	}
}
