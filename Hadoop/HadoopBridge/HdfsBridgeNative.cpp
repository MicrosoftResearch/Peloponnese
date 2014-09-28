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

#pragma unmanaged

#define DllExport __declspec(dllexport)

#include "JVMBridgeNative.h"
#include "HdfsBridgeNative.h"

#include <windows.h>
#include <assert.h>

using namespace Microsoft::Research::Peloponnese;

namespace Microsoft { namespace Research { namespace Peloponnese { namespace Hdfs
{
	struct Env
	{
		JNIEnv* e;
	};

	class InstanceInternal
	{
	public:
		jclass     m_clsInstance;
		jobject    m_obj;
		Instance*  m_holder;
	};

	class FileStatInternal
	{
	public:
		jclass             m_clsFileStat;
		jobject            m_fileStat;
		jclass             m_clsBlockLocations;
		jobject            m_blockLocations;
		FileStat*          m_holder;
	};

	class ReaderInternal
	{
	public:
		jclass             m_clsReader;
		jobject            m_reader;
		jclass             m_clsReaderBlock;
		Reader*            m_holder;
	};

	class WriterInternal
	{
	public:
		jclass             m_clsWriter;
		jobject            m_writer;
		Writer*            m_holder;
	};

	HdfsBlockLocInfo::HdfsBlockLocInfo()
	{
		numberOfHosts = 0;
		Hosts = NULL;
        Endpoints = NULL;
		Size = 0;
		Offset = 0;
	}

	HdfsBlockLocInfo::~HdfsBlockLocInfo()
	{
		for (long i=0; i<numberOfHosts; ++i)
		{
			free(Hosts[i]);
            free(Endpoints[i]);
		}
		delete [] Hosts;
		delete [] Endpoints;
	}

	bool OpenInstance(const char* schemeAndAuthority, Instance** pInstance)
	{
		JNIEnv* env = JVMBridge::AttachToJvm();

		jclass clsHdfsBridge = env->FindClass("com/microsoft/research/peloponnese/HdfsBridge");
		if (clsHdfsBridge == NULL)
		{
			fprintf(stderr, "Failed to find HdfsBridge class\n");fflush(stderr);
			return false;
		}

		jobject localInstance;
        if (schemeAndAuthority == NULL)
        {
    		jmethodID midOpenInstance = env->GetStaticMethodID(
	    		clsHdfsBridge, "OpenInstance", "()Lcom/microsoft/research/peloponnese/HdfsBridge$Instance;");
    		assert(midOpenInstance != NULL);

    		localInstance = env->CallStaticObjectMethod(clsHdfsBridge, midOpenInstance);
        }
        else
        {
    		jmethodID midOpenInstance = env->GetStaticMethodID(
	    		clsHdfsBridge, "OpenInstance",
		    	"(Ljava/lang/String;)Lcom/microsoft/research/peloponnese/HdfsBridge$Instance;");
    		assert(midOpenInstance != NULL);

            jstring jSchemeAndAuthority = env->NewStringUTF(schemeAndAuthority);

    		localInstance = env->CallStaticObjectMethod(clsHdfsBridge, midOpenInstance, jSchemeAndAuthority);
		    env->DeleteLocalRef(jSchemeAndAuthority);
        }

		if (localInstance == NULL)
		{
            fprintf(stderr, "Failed to open instance %s\n", (schemeAndAuthority == NULL) ? "default" : schemeAndAuthority);fflush(stderr);
			return false;
		}

		InstanceInternal* instance = new InstanceInternal();

		instance->m_clsInstance = env->FindClass("com/microsoft/research/peloponnese/HdfsBridge$Instance");
		assert(instance->m_clsInstance != NULL);

		instance->m_obj = env->NewGlobalRef(localInstance);
		env->DeleteLocalRef(localInstance);

		Instance* holder = new Instance();
		holder->p = instance;
		instance->m_holder = holder;
		*pInstance = holder;

		return true;
	};

	InstanceAccessor::InstanceAccessor(Instance* instance)
	{
		m_env = new Env;
        m_env->e = JVMBridge::AttachToJvm();
		m_inst = (InstanceInternal* ) instance->p;
	}

	InstanceAccessor::~InstanceAccessor()
	{
		delete m_env;
	}

	void InstanceAccessor::Discard()
	{
		if (m_inst->m_obj != NULL)
		{
			m_env->e->DeleteGlobalRef(m_inst->m_obj);
		}
		delete m_inst->m_holder;
		delete m_inst;
	}

	char* InstanceAccessor::GetExceptionMessage()
	{
        return JVMBridge::GetExceptionMessageLocal(m_env->e, m_inst->m_clsInstance, m_inst->m_obj);
	}

	bool InstanceAccessor::IsFileExists(const char* fileName, bool* pExists)
	{
		jmethodID midIsFileExist =
			m_env->e->GetMethodID(m_inst->m_clsInstance, "IsFileExist", "(Ljava/lang/String;)I");                         
		assert(midIsFileExist != NULL);

		jstring jFileName = m_env->e->NewStringUTF(fileName);
		jint jFileExists =
			m_env->e->CallIntMethod(m_inst->m_obj, midIsFileExist, jFileName);
		m_env->e->DeleteLocalRef(jFileName);

		if (jFileExists == -1)
		{
			return false;
		}
		else
		{
			*pExists = (jFileExists == 1) ? true : false;
			return true;
		}
	}

    bool InstanceAccessor::SetOwnerAndPermission(const char* path, const char* user, const char* group, short permission)
    {
		jmethodID midSetOwnerAndPermission =
			m_env->e->GetMethodID(m_inst->m_clsInstance,
            "SetOwnerAndPermission",
            "(Ljava/lang/String;Ljava/lang/String;Ljava/lang/String;S)I");
		assert(midSetOwnerAndPermission != NULL);

		jstring jPath = m_env->e->NewStringUTF(path);
		jstring jUser = (user == NULL) ? NULL : m_env->e->NewStringUTF(user);
		jstring jGroup = (group == NULL) ? NULL : m_env->e->NewStringUTF(group);
		jshort jPermission = permission;
        jint jSuccess = m_env->e->CallIntMethod(m_inst->m_obj, midSetOwnerAndPermission, jPath, jUser, jGroup, jPermission);
		m_env->e->DeleteLocalRef(jPath);
        if (user != NULL)
        {
    		m_env->e->DeleteLocalRef(jUser);
        }
        if (group != NULL)
        {
    		m_env->e->DeleteLocalRef(jGroup);
        }

        return (jSuccess == 1);
    }

	bool InstanceAccessor::EnsureDirectory(const char* path)
	{
		jmethodID midEnsureDirectory =
			m_env->e->GetMethodID(m_inst->m_clsInstance, "EnsureDirectory", "(Ljava/lang/String;)I");
		assert(midEnsureDirectory != NULL);

		jstring jPath = m_env->e->NewStringUTF(path);
        jint jSuccess = m_env->e->CallIntMethod(m_inst->m_obj, midEnsureDirectory, jPath);
		m_env->e->DeleteLocalRef(jPath);

        return (jSuccess == 1);
	}

	bool InstanceAccessor::DeleteFileOrDir(const char* fileName, bool recursive, bool* pDeleted)
	{
		jmethodID midDeleteFile =
			m_env->e->GetMethodID(m_inst->m_clsInstance, "DeleteFile", "(Ljava/lang/String;Z)I");                         
		assert(midDeleteFile != NULL);

		jstring jFileName = m_env->e->NewStringUTF(fileName);
		jboolean jRecursive = (jboolean) recursive;
		jint jFileDeleted =
			m_env->e->CallIntMethod(m_inst->m_obj, midDeleteFile, jFileName, jRecursive);
		m_env->e->DeleteLocalRef(jFileName);

		if (jFileDeleted == -1)
		{
			return false;
		}
		else
		{
			*pDeleted = (jFileDeleted == 1) ? true : false;
			return true;
		}
	}

	bool InstanceAccessor::RenameFileOrDir(const char* dstFileName, const char* srcFileName, bool* pRenamed)
	{
		jmethodID midRenameFile =
			m_env->e->GetMethodID(m_inst->m_clsInstance, "RenameFile", "(Ljava/lang/String;Ljava/lang/String;)I");                         
		assert(midRenameFile != NULL);

		jstring jDstFileName = m_env->e->NewStringUTF(dstFileName);
		jstring jSrcFileName = m_env->e->NewStringUTF(srcFileName);
		jint jFileRenamed =
			m_env->e->CallIntMethod(m_inst->m_obj, midRenameFile, jDstFileName, jSrcFileName);
		m_env->e->DeleteLocalRef(jDstFileName);
		m_env->e->DeleteLocalRef(jSrcFileName);

		if (jFileRenamed == -1)
		{
			return false;
		}
		else
		{
			*pRenamed = (jFileRenamed == 1) ? true : false;
			return true;
		}
	}

    bool InstanceAccessor::GetContentSummary(
        const char* fileName,
        long long* outLength,
        long long* outNumberOfFiles)
    {
		jclass clsContentSummary = m_env->e->FindClass("org/apache/hadoop/fs/ContentSummary");
        assert(clsContentSummary != NULL);

		jmethodID midOpenContentSummary = m_env->e->GetMethodID(
			m_inst->m_clsInstance,
			"OpenContentSummary",
			"(Ljava/lang/String;)Lorg/apache/hadoop/fs/ContentSummary;");  
        assert(midOpenContentSummary != NULL);

        jstring jFileName = m_env->e->NewStringUTF(fileName);
		jobject localContentSummary = m_env->e->CallObjectMethod(m_inst->m_obj, midOpenContentSummary, jFileName);
		m_env->e->DeleteLocalRef(jFileName);

        if (localContentSummary == NULL)
        {
            *outLength = -1;
            *outNumberOfFiles = -1;
            return false;
        }

		jmethodID midGetLength = m_env->e->GetMethodID(clsContentSummary, "getLength", "()J");
        assert(midGetLength != NULL);
        jlong jLength = m_env->e->CallLongMethod(localContentSummary, midGetLength);

		jmethodID midGetFileCount = m_env->e->GetMethodID(clsContentSummary, "getFileCount", "()J");
        assert(midGetFileCount != NULL);
        jlong jFileCount = m_env->e->CallLongMethod(localContentSummary, midGetFileCount);

        m_env->e->DeleteLocalRef(localContentSummary);

        *outLength = jLength;
        *outNumberOfFiles = jFileCount;

        return true;
    }

	bool InstanceAccessor::OpenFileStat(
		const char* fileName,
		bool getBlockArray,
		FileStat** pFileStat)
	{
		FileStatInternal* fs = new FileStatInternal();

		fs->m_clsFileStat = m_env->e->FindClass("org/apache/hadoop/fs/FileStatus");
		assert(fs->m_clsFileStat != NULL);

		fs->m_clsBlockLocations = m_env->e->FindClass("com/microsoft/research/peloponnese/HdfsBridge$Instance$BlockLocations");
		assert(fs->m_clsBlockLocations != NULL);

		jmethodID midOpenFileStat = m_env->e->GetMethodID(
			m_inst->m_clsInstance,
			"OpenFileStatus",
			"(Ljava/lang/String;)Lorg/apache/hadoop/fs/FileStatus;");  
		assert(midOpenFileStat != NULL);

		fs->m_fileStat = NULL;
		fs->m_blockLocations = NULL;

		jstring jFileName = m_env->e->NewStringUTF(fileName);
		jobject localFileStat = m_env->e->CallObjectMethod(m_inst->m_obj, midOpenFileStat, jFileName);
		m_env->e->DeleteLocalRef(jFileName);

		if (localFileStat == NULL)
		{
			delete fs;
			return false;
		}
		else
		{
			fs->m_fileStat = m_env->e->NewGlobalRef(localFileStat);
			m_env->e->DeleteLocalRef(localFileStat);
		}

		jmethodID midOpenBlockLocations = m_env->e->GetMethodID(
			m_inst->m_clsInstance,
			"OpenBlockLocations",
			"(Lorg/apache/hadoop/fs/FileStatus;Z)Lcom/microsoft/research/peloponnese/HdfsBridge$Instance$BlockLocations;");  
		assert(midOpenBlockLocations != NULL);

		jboolean jGetBlockArray = (jboolean) getBlockArray;
		jobject localBlockLoc =
			m_env->e->CallObjectMethod(
			m_inst->m_obj,
			midOpenBlockLocations,
			fs->m_fileStat, jGetBlockArray);

		if (localBlockLoc == NULL)
		{
			m_env->e->DeleteGlobalRef(fs->m_fileStat);
			delete fs;
			return false;
		}
		else
		{
			fs->m_blockLocations = m_env->e->NewGlobalRef(localBlockLoc);
			m_env->e->DeleteLocalRef(localBlockLoc);
		}

		FileStat* fileStat = new FileStat();
		fileStat->p = fs;
		fs->m_holder = fileStat;
		*pFileStat = fileStat;

		return true;
	}

	bool InstanceAccessor::EnumerateSubdirectories(const char* directoryName, char*** pDirectoryArray, int* pNumberOfDirectories)
	{
		jmethodID mid = m_env->e->GetMethodID(
			m_inst->m_clsInstance, "EnumerateSubdirectories", "(Ljava/lang/String;)[Ljava/lang/String;");
		assert(mid != NULL);

		char** array = NULL;

		jstring jDirectoryName = m_env->e->NewStringUTF(directoryName);
		jobjectArray nameArray = (jobjectArray)m_env->e->CallObjectMethod(m_inst->m_obj, mid, jDirectoryName);
		m_env->e->DeleteLocalRef(jDirectoryName);

		if (nameArray == NULL)
		{
			*pNumberOfDirectories = 0;
			*pDirectoryArray = NULL;

			return false;
		}
		else
		{
			jsize jArrayLength = m_env->e->GetArrayLength(nameArray);
			array = new char*[jArrayLength];
			*pNumberOfDirectories = jArrayLength;
			*pDirectoryArray = array;

			const char *fileName;
			for (int i = 0; i<jArrayLength; ++i)
			{
				jstring jFile = (jstring)m_env->e->GetObjectArrayElement(nameArray, i);
				fileName = (const char*)m_env->e->GetStringUTFChars(jFile, NULL);
				const char* trimPtr = fileName;
				while (*trimPtr == '/')
				{
					++trimPtr;
				}
				array[i] = _strdup(trimPtr);
				m_env->e->ReleaseStringUTFChars(jFile, fileName);
				m_env->e->DeleteLocalRef(jFile);
			}

			m_env->e->DeleteLocalRef(nameArray);

			return true;
		}
	}

	void InstanceAccessor::DisposeDirectoryArray(int length, char** array)
	{
		for (int i = 0; i<length; ++i)
		{
			free(array[i]);
		}
		delete[] array;
	}

	bool InstanceAccessor::OpenReader(const char* fileName, Reader** pReader)
	{
		ReaderInternal* r = new ReaderInternal;

		r->m_clsReader = m_env->e->FindClass("com/microsoft/research/peloponnese/HdfsBridge$Instance$Reader");
		assert(r->m_clsReader != NULL);

		r->m_clsReaderBlock = m_env->e->FindClass("com/microsoft/research/peloponnese/HdfsBridge$Instance$Reader$Block");
		assert(r->m_clsReaderBlock != NULL);

		jmethodID midOpenReader = m_env->e->GetMethodID(
			m_inst->m_clsInstance,
			"OpenReader",
			"(Ljava/lang/String;)Lcom/microsoft/research/peloponnese/HdfsBridge$Instance$Reader;");  
		assert(midOpenReader != NULL);

		jstring jFileName = m_env->e->NewStringUTF(fileName);
		jobject localReader = m_env->e->CallObjectMethod(m_inst->m_obj, midOpenReader, jFileName);
		m_env->e->DeleteLocalRef(jFileName);

		if (localReader == NULL)
		{
			delete r;
			return false;
		}
		else
		{
			r->m_reader = m_env->e->NewGlobalRef(localReader);
			m_env->e->DeleteLocalRef(localReader);
		}

		Reader* reader = new Reader();
		reader->p = r;
		r->m_holder = reader;
		*pReader = reader;

		return true;
	}

	bool InstanceAccessor::OpenCreate(const char* fileName, int bufferSize, long long blockSize, Writer** pWriter)
	{
		WriterInternal* w = new WriterInternal;

		w->m_clsWriter = m_env->e->FindClass("com/microsoft/research/peloponnese/HdfsBridge$Instance$Writer");
		assert(w->m_clsWriter != NULL);

		jmethodID midOpenCreate = m_env->e->GetMethodID(
			m_inst->m_clsInstance,
			"OpenCreate",
			"(Ljava/lang/String;IJ)Lcom/microsoft/research/peloponnese/HdfsBridge$Instance$Writer;");  
		assert(midOpenCreate != NULL);

		jstring jFileName = m_env->e->NewStringUTF(fileName);
        jint jbufferSize = (jint) bufferSize;
        jlong jblockSize = (jlong) blockSize;
        jobject localWriter = m_env->e->CallObjectMethod(m_inst->m_obj, midOpenCreate, jFileName, jbufferSize, jblockSize);
		m_env->e->DeleteLocalRef(jFileName);

		if (localWriter == NULL)
		{
			delete w;
			return false;
		}
		else
		{
			w->m_writer = m_env->e->NewGlobalRef(localWriter);
			m_env->e->DeleteLocalRef(localWriter);
		}

		Writer* writer = new Writer();
		writer->p = w;
		w->m_holder = writer;
		*pWriter = writer;

		return true;
	}

	bool InstanceAccessor::OpenAppend(const char* fileName, Writer** pWriter)
	{
		WriterInternal* w = new WriterInternal;

		w->m_clsWriter = m_env->e->FindClass("com/microsoft/research/peloponnese/HdfsBridge$Instance$Writer");
		assert(w->m_clsWriter != NULL);

		jmethodID midOpenAppend = m_env->e->GetMethodID(
			m_inst->m_clsInstance,
			"OpenAppend",
			"(Ljava/lang/String;)Lcom/microsoft/research/peloponnese/HdfsBridge$Instance$Writer;");  
		assert(midOpenAppend != NULL);

		jstring jFileName = m_env->e->NewStringUTF(fileName);
        jobject localWriter = m_env->e->CallObjectMethod(m_inst->m_obj, midOpenAppend, jFileName);
		m_env->e->DeleteLocalRef(jFileName);

		if (localWriter == NULL)
		{
			delete w;
			return false;
		}
		else
		{
			w->m_writer = m_env->e->NewGlobalRef(localWriter);
			m_env->e->DeleteLocalRef(localWriter);
		}

		Writer* writer = new Writer();
		writer->p = w;
		w->m_holder = writer;
		*pWriter = writer;

		return true;
	}

	FileStatAccessor::FileStatAccessor(FileStat* fileStat)
	{
		m_env = new Env;
        m_env->e = JVMBridge::AttachToJvm();
		m_stat = (FileStatInternal *) fileStat->p;
	}

	FileStatAccessor::~FileStatAccessor()
	{
		delete m_env;
	}

	void FileStatAccessor::Discard()
	{
		if (m_stat->m_fileStat != NULL)
		{
			m_env->e->DeleteGlobalRef(m_stat->m_fileStat);
			m_stat->m_fileStat = NULL;
		}

		if (m_stat->m_blockLocations != NULL)
		{
			m_env->e->DeleteGlobalRef(m_stat->m_blockLocations);
			m_stat->m_blockLocations = NULL;
		}

		delete m_stat->m_holder;
		delete m_stat;
	}

	char* FileStatAccessor::GetExceptionMessage()
	{
		return JVMBridge::GetExceptionMessageLocal(m_env->e, m_stat->m_clsFileStat, m_stat->m_fileStat);
	}

	char* FileStatAccessor::GetBlockExceptionMessage()
	{
		return JVMBridge::GetExceptionMessageLocal(m_env->e, m_stat->m_clsBlockLocations, m_stat->m_blockLocations);
	}

	long long FileStatAccessor::GetFileLength()
	{
		jmethodID mid = m_env->e->GetMethodID(
			m_stat->m_clsFileStat, "getLen", "()J");
		assert(mid != NULL);

		return m_env->e->CallLongMethod(m_stat->m_fileStat, mid);
	}

	bool FileStatAccessor::IsDir()
	{
		jmethodID mid = m_env->e->GetMethodID(
			m_stat->m_clsFileStat, "isDir", "()Z");
		assert(mid != NULL);

		jboolean isDir = m_env->e->CallBooleanMethod(m_stat->m_fileStat, mid);
		return (isDir) ? true : false;
	}

	long long FileStatAccessor::GetFileLastModified()
	{
		jmethodID mid = m_env->e->GetMethodID(
			m_stat->m_clsFileStat, "getModificationTime", "()J");
		assert(mid != NULL);

		return m_env->e->CallLongMethod(m_stat->m_fileStat, mid);
	}

	short FileStatAccessor::GetFileReplication()
	{
		jmethodID mid = m_env->e->GetMethodID(
			m_stat->m_clsFileStat, "getReplication", "()S");
		assert(mid != NULL);

		return m_env->e->CallShortMethod(m_stat->m_fileStat, mid);
	}

	long long FileStatAccessor::GetFileBlockSize()
	{
		jmethodID mid = m_env->e->GetMethodID(
			m_stat->m_clsFileStat, "getBlockSize", "()J");
		assert(mid != NULL);

		return m_env->e->CallLongMethod(m_stat->m_fileStat, mid);
	}

	long FileStatAccessor::GetNumberOfBlocks()
	{
		jmethodID mid = m_env->e->GetMethodID(
			m_stat->m_clsBlockLocations, "GetNumberOfBlocks", "()I");
		assert(mid != NULL);

		return m_env->e->CallIntMethod(m_stat->m_blockLocations, mid);
	}

	HdfsBlockLocInfo* FileStatAccessor::GetBlockInfo(long blockId)
	{
		HdfsBlockLocInfo* block = new HdfsBlockLocInfo();

		jint jId = blockId;

		jmethodID mid = m_env->e->GetMethodID(
			m_stat->m_clsBlockLocations, "GetBlockLength", "(I)J");
		assert(mid != NULL);

		block->Size = m_env->e->CallLongMethod(m_stat->m_blockLocations, mid, jId);

		mid = m_env->e->GetMethodID(
			m_stat->m_clsBlockLocations, "GetBlockOffset", "(I)J");
		assert(mid != NULL);

		block->Offset = m_env->e->CallLongMethod(m_stat->m_blockLocations, mid, jId);

		mid = m_env->e->GetMethodID(
			m_stat->m_clsBlockLocations, "GetBlockHosts", "(I)[Ljava/lang/String;");
		assert(mid != NULL);

		jobjectArray hostArray = (jobjectArray) m_env->e->CallObjectMethod(m_stat->m_blockLocations, mid, jId);
		if (hostArray != NULL)
		{
			jsize jArrayLength = m_env->e->GetArrayLength(hostArray);
			block->numberOfHosts = jArrayLength;
			block->Hosts = new char* [jArrayLength];

			const char *hostName;
			for (int i=0; i<jArrayLength; ++i) 
			{
				jstring jHost = (jstring) m_env->e->GetObjectArrayElement(hostArray, i);           
				hostName = (const char*) m_env->e->GetStringUTFChars(jHost, NULL);
				block->Hosts[i] = _strdup(hostName);
				m_env->e->ReleaseStringUTFChars(jHost, hostName);
				m_env->e->DeleteLocalRef(jHost);
			}

			m_env->e->DeleteLocalRef(hostArray);
		}

		mid = m_env->e->GetMethodID(
			m_stat->m_clsBlockLocations, "GetBlockEndpoints", "(I)[Ljava/lang/String;");
		assert(mid != NULL);

		jobjectArray endpointArray = (jobjectArray) m_env->e->CallObjectMethod(m_stat->m_blockLocations, mid, jId);
		if (endpointArray != NULL)
		{
			jsize jArrayLength = m_env->e->GetArrayLength(endpointArray);
            assert(jArrayLength == block->numberOfHosts);
            block->Endpoints = new char* [jArrayLength];

			const char *endpoint;
			for (int i=0; i<jArrayLength; ++i) 
			{
				jstring jEndpoint = (jstring) m_env->e->GetObjectArrayElement(endpointArray, i);           
				endpoint = (const char*) m_env->e->GetStringUTFChars(jEndpoint, NULL);
                block->Endpoints[i] = _strdup(endpoint);
                m_env->e->ReleaseStringUTFChars(jEndpoint, endpoint);
                m_env->e->DeleteLocalRef(jEndpoint);
			}

            m_env->e->DeleteLocalRef(endpointArray);
		}

		mid = m_env->e->GetMethodID(
			m_stat->m_clsBlockLocations, "GetBlockFileId", "(I)I");
		assert(mid != NULL);

		block->fileIndex = m_env->e->CallIntMethod(m_stat->m_blockLocations, mid, jId);

		return block;
	}

	void FileStatAccessor::DisposeBlockInfo(HdfsBlockLocInfo* bi)
	{
		delete bi;
	}

	long long FileStatAccessor::GetTotalFileLength()
	{
		jfieldID fidSize = m_env->e->GetFieldID(
			m_stat->m_clsBlockLocations, "fileSize", "J");
		assert(fidSize != NULL);

		return m_env->e->GetLongField(m_stat->m_blockLocations, fidSize);
	}

	long FileStatAccessor::GetNumberOfFiles()
	{
		jmethodID mid = m_env->e->GetMethodID(
			m_stat->m_clsBlockLocations, "GetNumberOfFileNames", "()I");
		assert(mid != NULL);

		return m_env->e->CallIntMethod(m_stat->m_blockLocations, mid);
	}

	char** FileStatAccessor::GetFileNameArray()
	{
		jmethodID mid = m_env->e->GetMethodID(
			m_stat->m_clsBlockLocations, "GetFileNames", "()[Ljava/lang/String;");
		assert(mid != NULL);

		char** array = NULL;

		jobjectArray nameArray = (jobjectArray) m_env->e->CallObjectMethod(m_stat->m_blockLocations, mid);
		if (nameArray != NULL)
		{
			jsize jArrayLength = m_env->e->GetArrayLength(nameArray);
			array = new char* [jArrayLength];

			const char *fileName;
			for (int i=0; i<jArrayLength; ++i) 
			{
				jstring jFile = (jstring) m_env->e->GetObjectArrayElement(nameArray, i);           
				fileName = (const char*) m_env->e->GetStringUTFChars(jFile, NULL);
                const char* trimPtr = fileName;
                while (*trimPtr == '/')
                {
                    ++trimPtr;
                }
				array[i] = _strdup(trimPtr);
				m_env->e->ReleaseStringUTFChars(jFile, fileName);
				m_env->e->DeleteLocalRef(jFile);
			}

			m_env->e->DeleteLocalRef(nameArray);
		}

		return array;
	}

	void FileStatAccessor::DisposeFileNameArray(long length, char** array)
	{
		for (long i=0; i<length; ++i)
		{
			free(array[i]);
		}
		delete [] array;
	}


	ReaderAccessor::ReaderAccessor(Reader* reader)
	{
		m_env = new Env;
		m_env->e = JVMBridge::AttachToJvm();
		m_rdr = (ReaderInternal *) reader->p;
	}

	ReaderAccessor::~ReaderAccessor()
	{
		delete m_env;
	}

	void ReaderAccessor::Discard()
	{
		if (m_rdr->m_reader != NULL)
		{
			m_env->e->DeleteGlobalRef(m_rdr->m_reader);
		}

		delete m_rdr->m_holder;
		delete m_rdr;
	}

	char* ReaderAccessor::GetExceptionMessage()
	{
		return JVMBridge::GetExceptionMessageLocal(m_env->e, m_rdr->m_clsReader, m_rdr->m_reader);
	}

	long ReaderAccessor::ReadBlock(long long offset, unsigned char* buffer, long bufferLength)
	{
		jmethodID mid = m_env->e->GetMethodID(
			m_rdr->m_clsReader, "ReadBlock", "(JI)Lcom/microsoft/research/peloponnese/HdfsBridge$Instance$Reader$Block;");
		assert(mid != NULL);

		jlong jOffset = offset;
		jint jLength = bufferLength;
		jobject block = m_env->e->CallObjectMethod(m_rdr->m_reader, mid, jOffset, jLength);

		jfieldID fidret = m_env->e->GetFieldID(
			m_rdr->m_clsReaderBlock, "ret", "I");
		assert(fidret != NULL);

		jint bytesRead = m_env->e->GetIntField(block, fidret);

		assert(bytesRead <= bufferLength);

		if (bytesRead > 0)
		{
			jfieldID fid = m_env->e->GetFieldID(
				m_rdr->m_clsReaderBlock, "buffer", "[B");
			assert(fid != NULL);

			jbyteArray byteArray = (jbyteArray) m_env->e->GetObjectField(block, fid);
			assert(byteArray != NULL);

			jint arrayLength = m_env->e->GetArrayLength(byteArray);
			assert(arrayLength >= bytesRead);

			m_env->e->GetByteArrayRegion(byteArray, 0, bytesRead, (jbyte *) buffer);

			m_env->e->DeleteLocalRef(byteArray);
		}

		m_env->e->DeleteLocalRef(block);

		return bytesRead;
	}

	bool ReaderAccessor::Close()
	{
		jmethodID mid = m_env->e->GetMethodID(
			m_rdr->m_clsReader, "Close", "()I");

		assert(mid != NULL);

		jint ret = m_env->e->CallIntMethod(m_rdr->m_reader, mid);

		return (ret) ? true : false;
	}


	WriterAccessor::WriterAccessor(Writer* writer)
	{
		m_env = new Env;
		m_env->e = JVMBridge::AttachToJvm();
		m_wtr = (WriterInternal *) writer->p;
	}

	WriterAccessor::~WriterAccessor()
	{
		delete m_env;
	}

	void WriterAccessor::Discard()
	{
		if (m_wtr->m_writer != NULL)
		{
			m_env->e->DeleteGlobalRef(m_wtr->m_writer);
		}

		delete m_wtr->m_holder;
		delete m_wtr;
	}

	char* WriterAccessor::GetExceptionMessage()
	{
		return JVMBridge::GetExceptionMessageLocal(m_env->e, m_wtr->m_clsWriter, m_wtr->m_writer);
	}

	bool WriterAccessor::WriteBlock(unsigned char* buffer, long bufferLength, bool flushAfter)
	{
		jmethodID mid = m_env->e->GetMethodID(
			m_wtr->m_clsWriter, "WriteBlock", "([BZ)I");
		assert(mid != NULL);

		jboolean jFlush = (jboolean) flushAfter;
		jint jLength = bufferLength;
		jbyteArray jBuffer = m_env->e->NewByteArray(jLength);
		assert(jBuffer != NULL);

		m_env->e->SetByteArrayRegion(jBuffer, 0, jLength, (jbyte *) buffer);

		jint jRet = m_env->e->CallIntMethod(m_wtr->m_writer, mid, jBuffer, jFlush);

		m_env->e->DeleteLocalRef(jBuffer);

		return (jRet) ? true : false;
	}

	bool WriterAccessor::Sync()
	{
		jmethodID mid = m_env->e->GetMethodID(
			m_wtr->m_clsWriter, "Sync", "()I");

		assert(mid != NULL);

		jint ret = m_env->e->CallIntMethod(m_wtr->m_writer, mid);

		return (ret) ? true : false;
	}

	bool WriterAccessor::Close()
	{
		jmethodID mid = m_env->e->GetMethodID(
			m_wtr->m_clsWriter, "Close", "()I");

		assert(mid != NULL);

		jint ret = m_env->e->CallIntMethod(m_wtr->m_writer, mid);

		return (ret) ? true : false;
	}
} } } };

#if 0

#include <process.h>

struct ReadBlock
{
	HdfsBridgeNative::Reader* r;
	HdfsBridgeNative::Instance* i;
	long long                 o;
	const char*               f;
};

unsigned __stdcall ThreadFunc(void* arg)
{
	ReadBlock* block = (ReadBlock *) arg;

	HdfsBridgeNative::ReaderAccessor r(block->r);

	int bytesRead = 0;
	long long offset = 0;
	char* buffer = new char[256*1024];
	do
	{
		bytesRead = r.ReadBlock(offset, buffer, 256*1024);
		if (bytesRead > 0)
		{
			printf("Read from %s:%I64d:%d\n", block->f, offset, bytesRead);
			offset += bytesRead;
		}
		if (bytesRead < -1)
		{
			printf("%s: %s\n", block->f, r.GetExceptionMessage());
		}
		if (bytesRead == -1)
		{
			printf("EOF\n");
		}
	} while (bytesRead > -1);

	return 0;
}

int main(int argc, wchar_t** argv)
{
	bool ret = HdfsBridgeNative::Initialize();

	if (!ret)
	{
		printf("Failed to initialize\n");
		return 0;
	}

	HdfsBridgeNative::Instance* instance;
	ret = HdfsBridgeNative::OpenInstance("svc-d1-17", 9000, &instance);

	if (!ret)
	{
		printf("failed open\n");
		return 0;
	}

	HANDLE h[4];

	HdfsBridgeNative::InstanceAccessor bridge(instance);

	ReadBlock* r0 = new ReadBlock;
	r0->f = "/data/inputPart0.txt";

	ret = bridge.OpenReader(r0->f, &r0->r);

	if (!ret)
	{
		printf("%s failed open %s\n", r0->f, bridge.GetExceptionMessage());
		bridge.Discard();
		return 0;
	}

	h[0] = (HANDLE) _beginthreadex(
		NULL,
		0,
		ThreadFunc,
		r0,
		0,
		NULL
		);

	h[1] = (HANDLE) _beginthreadex(
		NULL,
		0,
		ThreadFunc,
		r0,
		0,
		NULL
		);

	ReadBlock* r1 = new ReadBlock;
	r1->f = "/data";

	HdfsBridgeNative::FileStat* fileStat;
	ret = bridge.OpenFileStat(r1->f, true, &fileStat);

	{
		HdfsBridgeNative::FileStatAccessor fs(fileStat);

		long long ll = fs.GetFileLength();
		ll = fs.GetFileLastModified();
		ll = fs.GetFileBlockSize();
		long l = fs.GetFileReplication();
		bool b = fs.IsDir();

		long nBlocks = fs.GetNumberOfBlocks();
		for (long i=0; i<nBlocks; ++i)
		{
			HdfsBridgeNative::HdfsBlockLocInfo* block = fs.GetBlockInfo(i);
			delete block;
		}

		ll = fs.GetTotalFileLength();

		long nFiles = fs.GetNumberOfFiles();
		char** fArray = fs.GetFileNameArray();
		fs.DisposeFileNameArray(nFiles, fArray);

		fs.Discard();
	}

	ret = bridge.OpenReader(r1->f, &r1->r);

	if (!ret)
	{
		printf("%s failed open %s\n", r1->f, bridge.GetExceptionMessage());
		bridge.Discard();
		return 0;
	}

	h[2] = (HANDLE) _beginthreadex(
		NULL,
		0,
		ThreadFunc,
		r1,
		0,
		NULL
		);

	h[3] = (HANDLE) _beginthreadex(
		NULL,
		0,
		ThreadFunc,
		r1,
		0,
		NULL
		);

	WaitForMultipleObjects(4, h, TRUE, INFINITE);

	HdfsBridgeNative::ReaderAccessor ra0(r0->r);
	ret = ra0.Close();
	ra0.Discard();

	HdfsBridgeNative::ReaderAccessor ra1(r1->r);
	ret = ra1.Close();
	ra1.Discard();

	bridge.Discard();

#if 0
	HdfsBridgeNative::Instance* instance;
	ret = HdfsBridgeNative::OpenInstance("svc-d1-17", 9000, &instance);

	{
		HdfsBridgeNative::InstanceAccessor bridge(instance);

		char* msg = bridge.GetExceptionMessage();
		free(msg);

		bool exists;
		ret = bridge.IsFileExists("data/foo", &exists);
		msg = bridge.GetExceptionMessage();
		free(msg);

		ret = bridge.IsFileExists("/data/inputPart0.txt", &exists);
		msg = bridge.GetExceptionMessage();
		free(msg);

		HdfsBridgeNative::FileStat* fileStat;

		ret = bridge.OpenFileStat("data/foo", false, &fileStat);
		msg = bridge.GetExceptionMessage();
		free(msg);

		ret = bridge.OpenFileStat("/data/inputPart0.txt", true, &fileStat);
		msg = bridge.GetExceptionMessage();
		free(msg);

		{
			HdfsBridgeNative::FileStatAccessor fs(fileStat);

			long long ll = fs.GetFileLength();
			ll = fs.GetFileLastModified();
			ll = fs.GetFileBlockSize();
			long l = fs.GetFileReplication();
			bool b = fs.IsDir();

			long nBlocks = fs.GetNumberOfBlocks();
			for (long i=0; i<nBlocks; ++i)
			{
				HdfsBlockLocInfo* block = fs.GetBlockInfo(i);
				delete block;
			}

			fs.Discard();
		}

		HdfsBridgeNative::Reader* reader;

		ret = bridge.OpenReader("data/foo", &reader);
		msg = bridge.GetExceptionMessage();
		free(msg);

		ret = bridge.OpenReader("/data/inputPart0.txt", &reader);
		msg = bridge.GetExceptionMessage();
		free(msg);

		{
			HdfsBridgeNative::ReaderAccessor r(reader);

			int bytesRead = 0;
			long long offset = 0;
			char* buffer = new char[256*1024];
			FILE* f;
			errno_t e = fopen_s(&f, "\\users\\misard\\foo.txt", "wb");
			do
			{
				bytesRead = r.ReadBlock(offset, buffer, 256*1024);
				if (bytesRead > 0)
				{
					printf("Read from %I64d:%d\n", offset, bytesRead);
					fwrite(buffer, 1, bytesRead, f);
					offset += bytesRead;
				}
				if (bytesRead < -1)
				{
					msg = r.GetExceptionMessage();
					printf("%s\n", msg);
					free(msg);
				}
				if (bytesRead == -1)
				{
					printf("EOF\n");
				}
			} while (bytesRead > -1);

			fclose(f);

			ret = r.Close();
			msg = r.GetExceptionMessage();
			free(msg);

			r.Discard();
		}

		bridge.Discard();
	}
#endif

	return 0;
}

#endif