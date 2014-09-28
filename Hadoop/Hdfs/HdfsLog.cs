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
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Microsoft.Research.Peloponnese.Hdfs
{
    public class HdfsLogAppendStream : Stream
    {
        private readonly Uri _logUri;
        private readonly HdfsInstance _hdfs;
        private readonly HdfsWriter _writer;

        private long _writePosition;

        public HdfsLogAppendStream(Uri logUri, bool deleteIfExists)
        {
            _logUri = logUri;
            string logPath = _logUri.AbsolutePath;
            string logDirectory = logPath.Substring(0, logPath.LastIndexOf('/'));
            _writePosition = 0;

            try
            {
                _hdfs = new HdfsInstance(_logUri);
                _hdfs.EnsureDirectory(logDirectory);
                if (_hdfs.IsFileExists(logPath))
                {
                    if (deleteIfExists)
                    {
                        _hdfs.DeleteFile(logPath, false);
                    }
                    else
                    {
                        throw new ApplicationException("Log file " + logUri.AbsoluteUri + " already exists");
                    }
                }
                _writer = _hdfs.OpenCreate(logPath, 1024*1024, -1);
            }
            catch (Exception e)
            {
                throw new ApplicationException("HdfsLogAppendStream failed to open writer", e);
            }
        }

        public bool IsThreadSafe { get { return false; } }

        public override bool CanRead { get { return false; } }
        public override bool CanSeek { get { return false; } }
        public override bool CanWrite { get { return true; } }
        public override long Length { get { return _writePosition; } }
        public override long Position { get { return _writePosition; } set { throw new NotImplementedException(); }  }

        public override int Read(byte[] buffer, int offset, int count) { throw new NotImplementedException(); }
        public override long Seek(long offset, SeekOrigin origin) { throw new NotImplementedException(); }
        public override void SetLength(long value) { throw new NotImplementedException(); }

        public override void Flush()
        {
            lock (this)
            {
                // this is very expensive since it closes and reopens the log stream
                _writer.Sync();
            }
        }

        public override void Write(byte[] buffer, int offset, int count)
        {
            lock (this)
            {
                if (count == 0)
                {
                    return;
                }

                _writer.WriteBlock(buffer, offset, count, true);

                _writePosition += count;
            }
        }

        protected override void Dispose(bool disposing)
        {
            if (disposing)
            {
                _writer.Dispose();
                _hdfs.Dispose();
            }
            base.Dispose(disposing);
        }
    }

    public class HdfsLogReaderStream : Stream
    {
        private readonly Uri _logUri;
        private readonly HdfsInstance _hdfs;
        private HdfsReader _cachedReader;

        private long _readPosition;

        public HdfsLogReaderStream(Uri logUri)
        {
            _logUri = logUri;
            _readPosition = 0; // this is the current read position

            try
            {
                _hdfs = new HdfsInstance(_logUri);
                string logPath = _logUri.AbsolutePath;
                _cachedReader = _hdfs.OpenReader(logPath);
            }
            catch (Exception e)
            {
                throw new ApplicationException("HdfsLogReaderStream failed to open reader", e);
            }
        }

        public bool IsThreadSafe { get { return false; } }

        public override bool CanRead { get { return true; } }
        public override bool CanSeek { get { return true; } }
        public override bool CanWrite { get { return false; } }
        public override long Length
        {
            get { return _hdfs.GetFileInfo(_logUri.AbsolutePath, false).Size; }
        }
        public override long Position { get { return _readPosition; } set { _readPosition = value; } }

        public override void SetLength(long value) { throw new NotImplementedException(); }
        public override void Write(byte[] buffer, int offset, int count) { throw new NotImplementedException(); }
        public override void Flush() { }

        public override int Read(byte[] buffer, int offset, int count)
        {
            int numBytesRead = _cachedReader.ReadBlock(_readPosition, buffer, offset, count);

            if (numBytesRead == 0 && offset < Length)
            {
                // the reader has a stale idea of the file length; reopen it and try again
                _cachedReader.Dispose();
                string logPath = _logUri.AbsolutePath;
                _cachedReader = _hdfs.OpenReader(logPath);
                numBytesRead = _cachedReader.ReadBlock(_readPosition, buffer, offset, count);
            }

            _readPosition += numBytesRead;
            return numBytesRead;
        }

        public override long Seek(long offset, SeekOrigin origin)
        {
            switch (origin)
            {
                case SeekOrigin.Begin:
                    if (offset >= Length)
                    {
                        throw new ApplicationException("HdfsLogReaderStream invalid seek");
                    }
                    _readPosition = offset;
                    break;
                case SeekOrigin.Current:
                    if (_readPosition + offset >= Length)
                    {
                        throw new ApplicationException("HdfsLogReaderStream invalid seek");
                    }
                    _readPosition += offset;
                    break;
                case SeekOrigin.End:
                    _readPosition = Length;
                    break;
                default:
                    break;
            }

            return _readPosition;
        }

        protected override void Dispose(bool disposing)
        {
            if (disposing)
            {
                _hdfs.Dispose();
            }
            base.Dispose(disposing);
        }
    }
}
