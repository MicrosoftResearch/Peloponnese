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

#pragma unmanaged

#if !(defined DllExport)
#  define DllExport
#endif

#include <stdio.h>
#include <string.h>
#include <stdlib.h>

//---------------------------------------------------------------------------------------------------

namespace Microsoft { namespace Research { namespace Peloponnese { namespace Hdfs
{
	struct DllExport Instance
	{
		void* p;
	};
	class InstanceInternal;

	struct DllExport FileStat
	{
		void* p;
	};
	class FileStatInternal;

	struct DllExport Reader
	{
		void* p;
	};
	class ReaderInternal;

	struct DllExport Writer
	{
		void* p;
	};
	class WriterInternal;

	struct Env;

	class DllExport HdfsBlockLocInfo
	{
	public:
		HdfsBlockLocInfo();
		~HdfsBlockLocInfo();

		long numberOfHosts; /* length of the hosts array */
		char** Hosts;       /* hosts storing block replicas, freed by destructor */
		char** Endpoints;   /* IPAddress:port storing block replicas, freed by destructor */
		long long Size;     /* the size of the block in bytes */
		long long Offset;   /* start offset of file associated with this block */
		int fileIndex;      /* which file in a directory this block is part of */
	};

	class DllExport FileStatAccessor
	{
	public:
		FileStatAccessor(FileStat* fileStat);
		~FileStatAccessor();

		void Discard();

		char* GetExceptionMessage();
		char* GetBlockExceptionMessage();

		long long GetFileLength();
		bool IsDir();
		long long GetFileLastModified();
		short GetFileReplication();
		long long GetFileBlockSize();

		long long GetTotalFileLength();
		long GetNumberOfBlocks();
		HdfsBlockLocInfo* GetBlockInfo(long blockId);
		void DisposeBlockInfo(HdfsBlockLocInfo* blockInfo);
		long GetNumberOfFiles();
		char** GetFileNameArray();
		void DisposeFileNameArray(long length, char** array);

	private:
		void* operator new( size_t );
		void* operator new[]( size_t );

		Env*                m_env;
		FileStatInternal*   m_stat;
	};

	class DllExport ReaderAccessor
	{
	public:
		ReaderAccessor(Reader* reader);
		~ReaderAccessor();

		void Discard();

		char* GetExceptionMessage();

		long ReadBlock(long long offset, unsigned char* buffer, long bufferSize);

		bool Close();

	private:
		void* operator new( size_t );
		void* operator new[]( size_t );

		Env*              m_env;
		ReaderInternal*   m_rdr;
	};

	class DllExport WriterAccessor
	{
	public:
		WriterAccessor(Writer* writer);
		~WriterAccessor();

		void Discard();

		char* GetExceptionMessage();

		bool WriteBlock(unsigned char* buffer, long bufferSize, bool flushAfter);

        // this is an expensive operation that closes the file (sealing the current block) and then reopens it
        bool Sync();

		bool Close();

	private:
		void* operator new( size_t );
		void* operator new[]( size_t );

		Env*              m_env;
		WriterInternal*   m_wtr;
	};

	class DllExport InstanceAccessor
	{
	public:
		InstanceAccessor(Instance* instance);
		~InstanceAccessor();

		void Discard();

		char* GetExceptionMessage();

		bool IsFileExists(const char* fileName, bool* pExists);

		bool EnsureDirectory(const char* path);

        bool SetOwnerAndPermission(const char* path, const char* user, const char* group, short permission);

		bool DeleteFileOrDir(const char* fileName, bool recursive, bool* pDeleted);

		bool RenameFileOrDir(const char* dstFileName, const char* srcFileName, bool* pRenamed);

		bool OpenFileStat(const char* fileName, bool getBlockArray, FileStat** pFileStat);

		bool EnumerateSubdirectories(const char* directoryName, char*** pDirectoryArray, int* pNumberOfDirectories);

		void DisposeDirectoryArray(int numberOfDirectories, char** directoryArray);

		bool GetContentSummary(const char* fileName, long long* outLength, long long* outNumberOfFiles);

		bool OpenReader(const char* fileName, Reader** pReader);

		bool OpenCreate(const char* fileName, int bufferSize, long long blockSize, Writer** pWriter);

		bool OpenAppend(const char* fileName, Writer** pWriter);

	private:
		void* operator new( size_t );
		void* operator new[]( size_t );

		Env*                m_env;
		InstanceInternal*   m_inst;
	};

	DllExport bool OpenInstance(const char* schemeAndAuthority, Instance** pInstance);
} } } };
//---------------------------------------------------------------------------------------------------

