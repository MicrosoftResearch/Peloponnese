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

// HdfsBridgeManaged.cpp : main project file.

//#include "stdafx.h"

#pragma managed

#pragma warning( disable: 4793 )

#include "JVMBridgeNative.h"
#include "JVMBridgeManaged.h"

#include "HdfsBridgeNative.h"
#include "HdfsBridgeManaged.h"

//#include <string>
//#include <iostream>


#define SUCCESS 0
#define FAILURE -1

using namespace System::IO;
using namespace System::Runtime::InteropServices;
using namespace Microsoft::Research::Peloponnese;


//---------------------------------------------------------------------------------------------------
#if 0
int main(array<System::String ^> ^args)
{
	Console::WriteLine(L"Hello World");

	Uri^ headUri = gcnew Uri("hpchdfs://svc-d1-17:9000/");
	String^ hdfsFileSetName = "/data";
	String^ hdfsFileSetName1 = "/data/inputPart1.txt";

	bool ret = HdfsInstance::Initialize();

	HdfsInstance^ managedHdfsClient1 = gcnew HdfsInstance(headUri);

	HdfsFileInfo^ managedFileInfo = managedHdfsClient1->GetFileInfo(hdfsFileSetName, true);

	HdfsInstance^ managedHdfsClient2 = gcnew HdfsInstance(headUri);
	bool exists = managedHdfsClient2->IsFileExists(hdfsFileSetName1);

	//String^ hdfsFileSetName3 = "/data/tpch/customer_1G_128MB.txt";
	//HdfsInstance^ managedHdfsClient3 = gcnew HdfsInstance(headNode, hdfsPort);
	//array<HdfsBlockInfo^>^ blocks = managedHdfsClient3->GetBlocks(hdfsFileSetName3);

	//String^ hdfsFileSetName4 = "/data/tpch/customer_1G_128MB.txt";
	//HdfsInstance^ managedHdfsClient4 = gcnew HdfsInstance(headNode, hdfsPort);
	//String^ blockContent = managedHdfsClient4->ReadBlock(hdfsFileSetName4, 0, 0);

	//String^ hdfsFileSetName3 = "/data/tpch/customer_1G_128MB.txt";
	//HdfsInstance^ managedHdfsClient6 = gcnew HdfsInstance(headNode, hdfsPort);
	//bool fileExists2 = managedHdfsClient6->IsFileExists(hdfsFileSetName3);

	//String^ hdfsFileSetName7 = "/data/tpch/customer_1G_128MB.txt";
	//HdfsInstance^ managedHdfsClient7 = gcnew HdfsInstance(headNode, hdfsPort);
	//bool fileExists3 = managedHdfsClient7->IsFileExists(hdfsFileSetName7);

	//String^ hdfsFileSetName8 = "/data/tpch/customer_1G_128MB.txt";
	//HdfsInstance^ managedHdfsClient8 = gcnew HdfsInstance(headNode, hdfsPort);
	//bool fileExists4 = managedHdfsClient8->IsFileExists(hdfsFileSetName8);

	return 0;
}
#endif

namespace Microsoft { namespace Research { namespace Peloponnese { namespace Hdfs
{
	HdfsInstance::HdfsInstance(Uri^ hdfsUri)
	{
        if (!Hadoop::Initialize())
        {
            throw gcnew ApplicationException("Can't initialize Hadoop bridge");
        }

        System::String^ schemeAndAuthority = hdfsUri->GetLeftPart(UriPartial::Authority);
        bool ret = Open(schemeAndAuthority);

		if (!ret)
		{
			throw gcnew 
                ApplicationException(String::Format("Unable to connect to Hdfs at {0}", schemeAndAuthority));
		}

        m_serviceUri = schemeAndAuthority + "/";
	}

	HdfsInstance::~HdfsInstance()
	{
        this->!HdfsInstance();
	}

    HdfsInstance::!HdfsInstance()
    {
        Close();
    }

	bool HdfsInstance::Open(String^ schemeAndAuthority)
	{
        char* cSchemeAndAuthority = (char *) Marshal::StringToHGlobalAnsi(schemeAndAuthority).ToPointer();

		Instance* instance;
        bool ret = OpenInstance(cSchemeAndAuthority, &instance);

		Marshal::FreeHGlobal(IntPtr(cSchemeAndAuthority));

		if (ret)
		{
			m_instance = IntPtr(instance);
		}
		else
		{
			m_instance = IntPtr::Zero;
		}

		return ret;
	}

	void HdfsInstance::Close()
	{
		if (m_instance != IntPtr::Zero)
		{
			InstanceAccessor ia((Instance *) m_instance.ToPointer());

			ia.Discard();

			m_instance = IntPtr::Zero;
		}

		m_serviceUri = nullptr;
	}

    void HdfsInstance::SetOwnerAndPermission(String^ path, String^ user, String^ group, short permission)
    {
		// Marshal the managed strings to unmanaged memory.
		char* cPath = (char*) Marshal::StringToHGlobalAnsi(path).ToPointer();
		char* cUser = (user == nullptr) ? NULL : (char*) Marshal::StringToHGlobalAnsi(user).ToPointer();
		char* cGroup = (group == nullptr) ? NULL : (char*) Marshal::StringToHGlobalAnsi(group).ToPointer();

        InstanceAccessor ia((Instance *) m_instance.ToPointer());
        bool ret = ia.SetOwnerAndPermission(cPath, cUser, cGroup, permission);

		// free the unmanaged string.
		Marshal::FreeHGlobal(IntPtr(cPath));
        if (user != nullptr)
        {
    		Marshal::FreeHGlobal(IntPtr(cUser));
        }
        if (group != nullptr)
        {
    		Marshal::FreeHGlobal(IntPtr(cGroup));
        }

		if (!ret)
		{
			char* msg = ia.GetExceptionMessage();
			String^ errorMsg = Marshal::PtrToStringAnsi((IntPtr) msg);
			HadoopNative::DisposeString(msg);
			throw gcnew ApplicationException("Hdfs SetOwnerAndPermission: " + errorMsg);
		}
    }

    void HdfsInstance::EnsureDirectory(String^ path)
    {
		// Marshal the managed string to unmanaged memory.
		char* cPath = (char*) Marshal::StringToHGlobalAnsi(path).ToPointer();

        InstanceAccessor ia((Instance *) m_instance.ToPointer());
        bool ret = ia.EnsureDirectory(cPath);

		// free the unmanaged string.
		Marshal::FreeHGlobal(IntPtr(cPath));

		if (!ret)
		{
			char* msg = ia.GetExceptionMessage();
			String^ errorMsg = Marshal::PtrToStringAnsi((IntPtr) msg);
			HadoopNative::DisposeString(msg);
			throw gcnew ApplicationException("Hdfs EnsureDirectory: " + errorMsg);
		}
    }

    void HdfsInstance::GetContentSummary(String^ fileName, [Out] long long% length, [Out] long long% numberOfFiles)
    {
		// Marshal the managed string to unmanaged memory.
		char* cFileName = (char*) Marshal::StringToHGlobalAnsi(fileName).ToPointer();

		InstanceAccessor ia((Instance *) m_instance.ToPointer());

        long long nativeLength, nativeNFiles;
        bool ret = ia.GetContentSummary(cFileName, &nativeLength, &nativeNFiles);

		// free the unmanaged string.
		Marshal::FreeHGlobal(IntPtr(cFileName));

		if (!ret)
		{
			char* msg = ia.GetExceptionMessage();
			String^ errorMsg = Marshal::PtrToStringAnsi((IntPtr) msg);
			HadoopNative::DisposeString(msg);
			throw gcnew ApplicationException("Hdfs GetContentSummary: " + errorMsg);
		}

        length = nativeLength;
        numberOfFiles = nativeNFiles;
    }

	HdfsFileInfo^ HdfsInstance::GetFileInfo(String^ fileName, bool getBlockArray)
	{
		// Marshal the managed string to unmanaged memory.
		char* cFileName = (char*) Marshal::StringToHGlobalAnsi(fileName).ToPointer();

		InstanceAccessor ia((Instance *) m_instance.ToPointer());

		FileStat* fileStat;
		bool ret = ia.OpenFileStat(cFileName, getBlockArray, &fileStat);

		// free the unmanaged string.
		Marshal::FreeHGlobal(IntPtr(cFileName));

		if (!ret)
		{
			char* msg = ia.GetExceptionMessage();
			String^ errorMsg = Marshal::PtrToStringAnsi((IntPtr) msg);
			HadoopNative::DisposeString(msg);
			throw gcnew ApplicationException("Hdfs GetFileInfo: " + errorMsg);
		}

		FileStatAccessor fs(fileStat);

		HdfsFileInfo^ fileInfo = gcnew HdfsFileInfo();

		fileInfo->Name = fileName;
		fileInfo->IsDirectory = fs.IsDir();
		fileInfo->Size = fs.GetFileLength();
		fileInfo->LastModified = fs.GetFileLastModified();
		fileInfo->Replication = fs.GetFileReplication();
		fileInfo->BlockSize = fs.GetFileBlockSize();

		long numberOfFiles = fs.GetNumberOfFiles();
		fileInfo->fileNameArray = gcnew array<String^>(numberOfFiles);

		char** cArray = fs.GetFileNameArray();
		for (long i=0; i<numberOfFiles; ++i)
		{
			fileInfo->fileNameArray[i] = Marshal::PtrToStringAnsi((IntPtr) cArray[i]);
		}

		fs.DisposeFileNameArray(numberOfFiles, cArray);

		if (getBlockArray)
		{
			fileInfo->blockArray = gcnew array<HdfsBlockInfo^>(fs.GetNumberOfBlocks());

			for (long i=0; i<fileInfo->blockArray->Length; ++i)
			{
				HdfsBlockLocInfo* info = fs.GetBlockInfo(i);

				fileInfo->blockArray[i] = gcnew HdfsBlockInfo();

				fileInfo->blockArray[i]->fileIndex = info->fileIndex;
				fileInfo->blockArray[i]->Size = info->Size;
				fileInfo->blockArray[i]->Offset = info->Offset;
				fileInfo->blockArray[i]->Hosts = gcnew array<String^>(info->numberOfHosts);
				fileInfo->blockArray[i]->Endpoints = gcnew array<String^>(info->numberOfHosts);

				for (int j=0; j<info->numberOfHosts; ++j)
				{
					String^ h = Marshal::PtrToStringAnsi((IntPtr) info->Hosts[j]);
					fileInfo->blockArray[i]->Hosts[j] = h;
                    String^ e = Marshal::PtrToStringAnsi((IntPtr) info->Endpoints[j]);
                    fileInfo->blockArray[i]->Endpoints[j] = e;
				}

				fs.DisposeBlockInfo(info);
			}

			fileInfo->totalSize = fs.GetTotalFileLength();
		}

		fs.Discard();

		return fileInfo;
	}

	array<String^>^ HdfsInstance::EnumerateSubdirectories(String^ fileName)
	{
		// Marshal the managed string to unmanaged memory.
		char* cFileName = (char*)Marshal::StringToHGlobalAnsi(fileName).ToPointer();

		InstanceAccessor ia((Instance *)m_instance.ToPointer());

		char** directoryArray;
		int numberOfDirectories;
		bool ret = ia.EnumerateSubdirectories(cFileName, &directoryArray, &numberOfDirectories);

		// free the unmanaged string.
		Marshal::FreeHGlobal(IntPtr(cFileName));

		if (!ret)
		{
			char* msg = ia.GetExceptionMessage();
			String^ errorMsg = Marshal::PtrToStringAnsi((IntPtr)msg);
			HadoopNative::DisposeString(msg);
			throw gcnew ApplicationException("Hdfs EnumerateSubdirectories: " + errorMsg);
		}

		array<String^>^ directories = gcnew array<String^>(numberOfDirectories);
		for (long i = 0; i<numberOfDirectories; ++i)
		{
			directories[i] = Marshal::PtrToStringAnsi((IntPtr)directoryArray[i]);
		}

		ia.DisposeDirectoryArray(numberOfDirectories, directoryArray);

		return directories;
	}

    HdfsReader^ HdfsInstance::OpenReader(String^ fileName)
	{
		// Marshal the managed string to unmanaged memory.
		char* cFileName = (char*) Marshal::StringToHGlobalAnsi(fileName).ToPointer();

		InstanceAccessor ia((Instance *) m_instance.ToPointer());

		Reader* reader;
		bool ret = ia.OpenReader(cFileName, &reader);

		// free the unmanaged string.
		Marshal::FreeHGlobal(IntPtr(cFileName));

		if (!ret)
		{
			char* msg = ia.GetExceptionMessage();
			String^ errorMsg = Marshal::PtrToStringAnsi((IntPtr) msg);
			HadoopNative::DisposeString(msg);
			throw gcnew ApplicationException("Hdfs OpenReader: " + errorMsg);
		}

        return gcnew HdfsReader(IntPtr(reader));
	}

    array<unsigned char>^ HdfsInstance::ReadAll(String^ fileName)
    {
        HdfsFileInfo^ info = GetFileInfo(fileName, false);
        System::Diagnostics::Debug::Assert(info->Size < 0x80000000);
        long size = (long) info->Size;
        array<unsigned char>^ buffer = gcnew array<unsigned char>(size);
        
        HdfsReader^ reader = OpenReader(fileName);
        try
        {
            long offset = 0;
            long toRead = size;
            while (toRead > 0)
            {
                long nRead = reader->ReadBlock((long long) offset, buffer, offset, toRead);
                if (nRead == 0)
                {
                    throw gcnew ApplicationException("ReadBlock returned 0 bytes");
                }
                System::Diagnostics::Debug::Assert(nRead <= toRead);
                offset += nRead;
                toRead -= nRead;
            }
        }
        catch (Exception^ e)
        {
            delete reader;
            throw e;
        }

        delete reader;
        return buffer;
    }

    void HdfsInstance::DownloadAll(String^ srcURI, String^ dstPath)
    {
        static const long blockSize = 2 * 1024 * 1024;
        array<unsigned char>^ buffer = gcnew array<unsigned char>(blockSize);

        HdfsFileInfo^ info = GetFileInfo(srcURI, false);

        FileStream^ writer = gcnew FileStream(dstPath, FileMode::Create, FileAccess::Write);
        HdfsReader^ reader = OpenReader(srcURI);
        try
        {
            long long offset = 0;
            long long toRead = info->Size;
            while (toRead > 0)
            {
                long blockLength = (toRead < (long long)blockSize) ? (long)toRead : blockSize;
                long nRead = reader->ReadBlock(offset, buffer, 0, blockLength);
                if (nRead == 0)
                {
                    throw gcnew ApplicationException("ReadBlock returned 0 bytes");
                }
                System::Diagnostics::Debug::Assert(nRead <= blockLength);
                offset += (long long)nRead;
                toRead -= (long long)nRead;
                writer->Write(buffer, 0, nRead);
            }
        }
        catch (Exception^ e)
        {
            delete reader;
            delete writer;
            throw e;
        }

        delete reader;
        delete writer;
    }

    HdfsWriter^ HdfsInstance::OpenCreate(String^ fileName, int bufferSize, long long blockSize)
	{
		// Marshal the managed string to unmanaged memory.
		char* cFileName = (char*) Marshal::StringToHGlobalAnsi(fileName).ToPointer();

		InstanceAccessor ia((Instance *) m_instance.ToPointer());

		Writer* writer;
		bool ret = ia.OpenCreate(cFileName, bufferSize, blockSize, &writer);

		// free the unmanaged string.
		Marshal::FreeHGlobal(IntPtr(cFileName));

		if (!ret)
		{
			char* msg = ia.GetExceptionMessage();
			String^ errorMsg = Marshal::PtrToStringAnsi((IntPtr) msg);
			HadoopNative::DisposeString(msg);
			throw gcnew ApplicationException("Hdfs OpenCreate: " + errorMsg);
		}

        return gcnew HdfsWriter(IntPtr(writer));
	}

    HdfsWriter^ HdfsInstance::OpenAppend(String^ fileName)
	{
		// Marshal the managed string to unmanaged memory.
		char* cFileName = (char*) Marshal::StringToHGlobalAnsi(fileName).ToPointer();

		InstanceAccessor ia((Instance *) m_instance.ToPointer());

		Writer* writer;
		bool ret = ia.OpenAppend(cFileName, &writer);

		// free the unmanaged string.
		Marshal::FreeHGlobal(IntPtr(cFileName));

		if (!ret)
		{
			char* msg = ia.GetExceptionMessage();
			String^ errorMsg = Marshal::PtrToStringAnsi((IntPtr) msg);
			HadoopNative::DisposeString(msg);
			throw gcnew ApplicationException("Hdfs OpenAppend: " + errorMsg);
		}

        return gcnew HdfsWriter(IntPtr(writer));
	}

    void HdfsInstance::WriteAll(String^ fileName, array<unsigned char>^ buffer)
    {
        HdfsWriter^ writer = OpenCreate(fileName, Math::Min(buffer->Length, 1024*1024), -1);
        try
        {
            writer->WriteBlock(buffer, 0, buffer->Length, true);
        }
        catch (Exception^ e)
        {
            delete writer;
            throw e;
        }

        delete writer;
    }

    void HdfsInstance::UploadAll(String^ srcPath, String^ dstUri)
    {
        static const long blockSize = 2 * 1024 * 1024;
        array<unsigned char>^ buffer = gcnew array<unsigned char>(blockSize);

        FileStream^ reader = gcnew FileStream(srcPath, FileMode::Open, FileAccess::Read);
        HdfsWriter^ writer = OpenCreate(dstUri, 1024*1024, -1);
        try
        {
            while (true)
            {
                int nRead = reader->Read(buffer, 0, blockSize);
                if (nRead == 0)
                {
                    break;
                }
                writer->WriteBlock(buffer, 0, nRead, false);
            }
        }
        catch (Exception^ e)
        {
            delete reader;
            delete writer;
            throw e;
        }

        delete reader;
        delete writer;
    }

	bool HdfsInstance::IsFileExists(String^ fileName)
	{
		// Marshal the managed string to unmanaged memory.
		char* cFileName = (char*) Marshal::StringToHGlobalAnsi(fileName).ToPointer();

		InstanceAccessor ia((Instance *) m_instance.ToPointer());

		bool exists = false;
		bool result = ia.IsFileExists(cFileName, &exists);

		// free the unmanaged string.
		Marshal::FreeHGlobal(IntPtr(cFileName));

		if (!result)
		{
			char* msg = ia.GetExceptionMessage();
			String^ errorMsg = Marshal::PtrToStringAnsi((IntPtr) msg);
			HadoopNative::DisposeString(msg);
			throw gcnew ApplicationException("Hdfs IsFileExists: " + errorMsg);
		}

		return exists;
	}

	bool HdfsInstance::DeleteFile(String^ fileName, bool recursive)
	{
		// Marshal the managed string to unmanaged memory.
		char* cFileName = (char*) Marshal::StringToHGlobalAnsi(fileName).ToPointer();

		InstanceAccessor ia((Instance *) m_instance.ToPointer());

		bool deleted = false;
		bool result = ia.DeleteFileOrDir(cFileName, recursive, &deleted);

		// free the unmanaged string.
		Marshal::FreeHGlobal(IntPtr(cFileName));

		if (!result)
		{
			char* msg = ia.GetExceptionMessage();
			String^ errorMsg = Marshal::PtrToStringAnsi((IntPtr) msg);
			HadoopNative::DisposeString(msg);
			throw gcnew ApplicationException("Hdfs DeleteFile: " + errorMsg);
		}

		return deleted;
	}

	bool HdfsInstance::RenameFile(String^ dstFileName, String^ srcFileName)
	{
		// Marshal the managed strings to unmanaged memory.
		char* cDstFileName = (char*) Marshal::StringToHGlobalAnsi(dstFileName).ToPointer();
		char* cSrcFileName = (char*) Marshal::StringToHGlobalAnsi(srcFileName).ToPointer();

		InstanceAccessor ia((Instance *) m_instance.ToPointer());

		bool renamed;
		bool result = ia.RenameFileOrDir(cDstFileName, cSrcFileName, &renamed);

		// free the unmanaged strings.
		Marshal::FreeHGlobal(IntPtr(cSrcFileName));
		Marshal::FreeHGlobal(IntPtr(cDstFileName));

		if (!result)
		{
			char* msg = ia.GetExceptionMessage();
			String^ errorMsg = Marshal::PtrToStringAnsi((IntPtr) msg);
			HadoopNative::DisposeString(msg);
			throw gcnew ApplicationException("Hdfs RenameFile: " + errorMsg);
		}

		return renamed;
	}

	String^ HdfsInstance::FromInternalUri(String^ inputString)
	{
		if (inputString->StartsWith(m_serviceUri))
		{
			return inputString->Substring(m_serviceUri->Length);
		}
		else
		{
			throw gcnew ApplicationException(inputString + " doesn't start with " + m_serviceUri);
			return nullptr;
		}
	}

	String^ HdfsInstance::ToInternalUri(String^ inputString)
	{
		return m_serviceUri + inputString;
	}


    HdfsReader::HdfsReader(IntPtr instance)
    {
        m_instance = instance;
    }

    HdfsReader::~HdfsReader()
    {
        this->!HdfsReader();
    }

    HdfsReader::!HdfsReader()
    {
        if (m_instance != IntPtr::Zero)
        {
            ReaderAccessor ra((Reader *) m_instance.ToPointer());

            ra.Close();
            ra.Discard();

            m_instance = IntPtr::Zero;
        }
    }

    long HdfsReader::ReadBlock(long long fileOffset, array<unsigned char>^ buffer, long bufferOffset, long toRead)
    {
        ReaderAccessor ra((Reader *) m_instance.ToPointer());

        if (bufferOffset > buffer->Length || toRead > (buffer->Length - bufferOffset))
        {
            throw gcnew ArgumentOutOfRangeException(
                "Can't read " + toRead + " bytes from offset " + bufferOffset +
                " in a buffer of length " + buffer->Length);
        }

        long ret;
        {
            pin_ptr<unsigned char> buf = &(buffer[bufferOffset]);
            ret = ra.ReadBlock(fileOffset, buf, toRead);
        }

        if (ret == -1)
        {
            // this means end of stream in Hadoop land
            return 0;
        }
        else if (ret < 0)
        {
            String^ exception = gcnew String(ra.GetExceptionMessage());
            throw gcnew ApplicationException("Hdfs Reader error " + exception);
        }

        return ret;
    }

    HdfsWriter::HdfsWriter(IntPtr instance)
    {
        m_instance = instance;
    }

    HdfsWriter::~HdfsWriter()
    {
        this->!HdfsWriter();
    }

    HdfsWriter::!HdfsWriter()
    {
        if (m_instance != IntPtr::Zero)
        {
            WriterAccessor wa((Writer *) m_instance.ToPointer());

            wa.Close();
            wa.Discard();

            m_instance = IntPtr::Zero;
        }
    }

    void HdfsWriter::WriteBlock(array<unsigned char>^ buffer, long bufferOffset, long toWrite, bool flushAfter)
    {
        WriterAccessor wa((Writer *) m_instance.ToPointer());

        if (bufferOffset > buffer->Length || toWrite > (buffer->Length - bufferOffset))
        {
            throw gcnew ArgumentOutOfRangeException(
                "Can't write " + toWrite + " bytes from offset " + bufferOffset +
                " from a buffer of length " + buffer->Length);
        }

        bool ret;
        {
            pin_ptr<unsigned char> buf = &(buffer[bufferOffset]);
            ret = wa.WriteBlock(buf, toWrite, flushAfter);
        }

        if (!ret)
        {
            throw gcnew ApplicationException("Hdfs WriteBlock got error " + gcnew String(wa.GetExceptionMessage()));
        }
    }

    void HdfsWriter::Sync()
    {
        WriterAccessor wa((Writer *) m_instance.ToPointer());

        if (!wa.Sync())
        {
            throw gcnew ApplicationException("Hdfs sync got error " + gcnew String(wa.GetExceptionMessage()));
        }
    }
}}}}
