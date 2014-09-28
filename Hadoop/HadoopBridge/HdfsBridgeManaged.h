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


#pragma once
#pragma managed

using namespace System;
using namespace System::Collections::Generic;

namespace Microsoft { namespace Research { namespace Peloponnese { namespace Hdfs
{
	//---------------------------------------------------------------------------------------------------

	public ref class HdfsBlockInfo
	{
	public:
		array<String^>^ Hosts;
		array<String^>^ Endpoints;
		long long Size;
		long long Offset;
		int fileIndex;
	};

	//---------------------------------------------------------------------------------------------------

	public ref class HdfsFileInfo
	{
	public:
		String^ Name;
		bool IsDirectory;
		long long Size;
		long long LastModified;
		short Replication;
		long long BlockSize;

		long long totalSize;

		array<HdfsBlockInfo^>^ blockArray;
		array<String^>^ fileNameArray;
	};

	//---------------------------------------------------------------------------------------------------

    public ref class HdfsReader : public IDisposable
    {
    public:
        HdfsReader(IntPtr instance);
        ~HdfsReader();
        !HdfsReader();

        long ReadBlock(long long fileOffset, array<unsigned char>^ buffer, long bufferOffset, long toRead);

    private:
        IntPtr   m_instance;
    };

	//---------------------------------------------------------------------------------------------------

    public ref class HdfsWriter : public IDisposable
    {
    public:
        HdfsWriter(IntPtr instance);
        ~HdfsWriter();
        !HdfsWriter();

        void WriteBlock(array<unsigned char>^ buffer, long bufferOffset, long toWrite, bool flushAfter);

        // this is an expensive operation that closes the file (sealing the current block) and then reopens it
        void Sync();

    private:
        IntPtr   m_instance;
    };

	//---------------------------------------------------------------------------------------------------

	public ref class HdfsInstance : public IDisposable
	{
	public:
		HdfsInstance(Uri^ hdfsUri);
		~HdfsInstance();
        !HdfsInstance();

		void Close();

		bool IsFileExists(String^ fileName);

		void EnsureDirectory(String^ path);

        void SetOwnerAndPermission(String^ path, String^ user, String^ group, short permission);

		HdfsFileInfo^ GetFileInfo(String^ fileName, bool getBlockArray);

		array<String^>^ EnumerateSubdirectories(String^ fileName);

        void GetContentSummary(String^ fileName, long long% length, long long% numberOfFiles);

        HdfsReader^ OpenReader(String^ fileName);
        array<unsigned char>^ ReadAll(String^ fileName);
        void DownloadAll(String^ srcURI, String^ dstPath);

        HdfsWriter^ OpenCreate(String^ fileName, int bufferSize, long long blockSize);
        HdfsWriter^ OpenAppend(String^ fileName);
        void WriteAll(String^ fileName, array<unsigned char>^);
        void UploadAll(String^ srcPath, String^ dstPath);

		bool DeleteFile(String^ fileName, bool recursive);

		bool RenameFile(String^ dstFileName, String^ srcFileName);

		String^ ToInternalUri(String^ fileName);

		String^ FromInternalUri(String^ fileName);

	private:
		bool Open(String^ schemeAndAuthority);

		IntPtr   m_instance;
		String^  m_serviceUri;
	};

	//---------------------------------------------------------------------------------------------------
}}}}
