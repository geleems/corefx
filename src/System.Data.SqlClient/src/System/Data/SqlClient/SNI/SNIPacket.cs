// Licensed to the .NET Foundation under one or more agreements.
// The .NET Foundation licenses this file to you under the MIT license.
// See the LICENSE file in the project root for more information.

using System.Collections.Concurrent;
using System.IO;
using System.Threading;
using System.Threading.Tasks;

namespace System.Data.SqlClient.SNI
{
    /// <summary>
    /// SNI Packet
    /// </summary>
    internal class SNIPacket : IDisposable, IEquatable<SNIPacket>
    {
        private byte[] _data;
        private int _length;
        private int _offset;
        private string _description;
        private SNIAsyncCallback _completionCallback;

        private SNIPacketFactory _sniPacketFactory = SNIPacketFactory.Instance;

        /// <summary>
        /// Packet description (used for debugging)
        /// </summary>
        public string Description
        {
            get
            {
                return _description;
            }

            set
            {
                _description = value;
            }
        }

        public byte[] Data
        {
            get
            {
                return _data;
            }

            set
            {
                _data = value;
            }
        }

        /// <summary>
        /// Length of data left to process
        /// </summary>
        public int DataLeft
        {
            get
            {
                return _length - _offset;
            }
        }

        /// <summary>
        /// Length of data
        /// </summary>
        public int Length
        {
            get
            {
                return _length;
            }
        }

        public int Capacity
        {
            get
            {
                int capacity = -1;
                if (_data != null)
                {
                    capacity = _data.Length;
                }
                return capacity;
            }
        }

        /// <summary>
        /// Packet validity
        /// </summary>
        public bool IsInvalid
        {
            get
            {
                return _data == null;
            }
        }

        /// <summary>
        /// Packet data
        /// </summary>
        public void Dispose()
        {
            Release();
        }

        /// <summary>
        /// Set async completion callback
        /// </summary>
        /// <param name="completionCallback">Completion callback</param>
        public void SetCompletionCallback(SNIAsyncCallback completionCallback)
        {
            _completionCallback = completionCallback;
        }

        /// <summary>
        /// Invoke the completion callback 
        /// </summary>
        /// <param name="sniErrorCode">SNI error</param>
        public void InvokeCompletionCallback(uint sniErrorCode)
        {
            _completionCallback(this, sniErrorCode);
        }


        /// <summary>
        /// Allocate space for data
        /// </summary>
        /// <param name="bufferSize">Length of byte array to be allocated</param>
        public void Allocate(int bufferSize)
        {
            if (_data == null || _data.Length != bufferSize)
            {
                if (_sniPacketFactory != null)
                {
                    _data = _sniPacketFactory.GetSNIPacketBuffer(bufferSize);
                }
                else
                {
                    _data = new byte[bufferSize];
                }
            }

            _length = 0;
            _offset = 0;
        }

        /// <summary>
        /// Clone packet
        /// </summary>
        /// <returns>Cloned packet</returns>
        public SNIPacket Clone()
        {
            SNIPacket packet;
            if (_sniPacketFactory != null)
            {
                packet = _sniPacketFactory.GetSNIPacket(_data.Length);
            }
            else
            {
                packet = new SNIPacket();
                packet._data = new byte[_data.Length];
            }

            Buffer.BlockCopy(_data, 0, packet._data, 0, _data.Length);
            packet._length = _length;
            packet._description = _description;
            packet._completionCallback = _completionCallback;

            return packet;
        }

        /// <summary>
        /// Get packet data
        /// </summary>
        /// <param name="buffer">Buffer</param>
        /// <param name="dataSize">Data in packet</param>
        public void GetData(byte[] buffer, ref int dataSize)
        {
            Buffer.BlockCopy(_data, 0, buffer, 0, _length);
            dataSize = _length;
        }

        /// <summary>
        /// Set packet data
        /// </summary>
        /// <param name="data">Data</param>
        /// <param name="length">Length</param>
        public void SetData(byte[] data, int length)
        {
            _data = data;
            _length = length;
            _offset = 0;
        }

        /// <summary>
        /// Take data from another packet
        /// </summary>
        /// <param name="packet">Packet</param>
        /// <param name="size">Data to take</param>
        /// <returns>Amount of data taken</returns>
        public int TakeData(SNIPacket packet, int size)
        {
            int dataSize = TakeData(packet._data, packet._length, size);
            packet._length += dataSize;
            return dataSize;
        }

        /// <summary>
        /// Append data
        /// </summary>
        /// <param name="data">Data</param>
        /// <param name="size">Size</param>
        public void AppendData(byte[] data, int size)
        {
            Buffer.BlockCopy(data, 0, _data, _length, size);
            _length += size;
        }

        /// <summary>
        /// Append another packet
        /// </summary>
        /// <param name="packet">Packet</param>
        public void AppendPacket(SNIPacket packet)
        {
            Buffer.BlockCopy(packet._data, 0, _data, _length, packet._length);
            _length += packet._length;
        }

        /// <summary>
        /// Take data from packet and advance offset
        /// </summary>
        /// <param name="buffer">Buffer</param>
        /// <param name="dataOffset">Data offset</param>
        /// <param name="size">Size</param>
        /// <returns></returns>
        public int TakeData(byte[] buffer, int dataOffset, int size)
        {
            if (_offset >= _length)
            {
                return 0;
            }

            if (_offset + size > _length)
            {
                size = _length - _offset;
            }

            Buffer.BlockCopy(_data, _offset, buffer, dataOffset, size);
            _offset += size;
            return size;
        }

        /// <summary>
        /// Release packet
        /// </summary>
        public void Release()
        {
            if (_sniPacketFactory != null)
            {
                _sniPacketFactory.PutSNIPacket(this);
            }
            else
            {
                _data = null;
                Reset();
            }
        }

        /// <summary>
        /// Reset packet 
        /// </summary>
        public void Reset()
        {
            _length = 0;
            _offset = 0;
            _description = null;
            _completionCallback = null;
        }

        /// <summary>
        /// Read data from a stream asynchronously
        /// </summary>
        /// <param name="stream">Stream to read from</param>
        /// <param name="callback">Completion callback</param>
        public void ReadFromStreamAsync(Stream stream, SNIAsyncCallback callback, bool isMars)
        {
            bool error = false;
            TaskContinuationOptions options = TaskContinuationOptions.DenyChildAttach;
            // MARS operations during Sync ADO.Net API calls are Sync over Async. Each API call can request 
            // threads to execute the async reads. MARS operations do not get the threads quickly enough leading to timeout
            // To fix the MARS thread exhaustion issue LongRunning continuation option is a temporary solution with its own drawbacks, 
            // and should be removed after evaluating how to fix MARS threading issues efficiently
            if (isMars)
            {
                options |= TaskContinuationOptions.LongRunning;
            }

            stream.ReadAsync(_data, 0, _data.Length).ContinueWith(t =>
            {
                Exception e = t.Exception != null ? t.Exception.InnerException : null;
                if (e != null)
                {
                    SNILoadHandle.SingletonInstance.LastError = new SNIError(SNIProviders.TCP_PROV, SNICommon.InternalExceptionError, e);
                    error = true;
                }
                else
                {
                    _length = t.Result;

                    if (_length == 0)
                    {
                        SNILoadHandle.SingletonInstance.LastError = new SNIError(SNIProviders.TCP_PROV, 0, SNICommon.ConnTerminatedError, string.Empty);
                        error = true;
                    }
                }

                if (error)
                {
                    Release();
                }

                callback(this, error ? TdsEnums.SNI_ERROR : TdsEnums.SNI_SUCCESS);
            },
            CancellationToken.None,
            options,
            TaskScheduler.Default);
        }

        /// <summary>
        /// Read data from a stream synchronously
        /// </summary>
        /// <param name="stream">Stream to read from</param>
        public void ReadFromStream(Stream stream)
        {
            _length = stream.Read(_data, 0, _data.Length);
        }

        /// <summary>
        /// Write data to a stream synchronously
        /// </summary>
        /// <param name="stream">Stream to write to</param>
        public void WriteToStream(Stream stream)
        {
            stream.Write(_data, 0, _length);
        }

        public Task WriteToStreamAsync(Stream stream)
        {
            return stream.WriteAsync(_data, 0, _length);
        }

        /// <summary>
        /// Get hash code
        /// </summary>
        /// <returns>Hash code</returns>
        public override int GetHashCode()
        {
            return base.GetHashCode();
        }

        /// <summary>
        /// Check packet equality
        /// </summary>
        /// <param name="obj"></param>
        /// <returns>true if equal</returns>
        public override bool Equals(object obj)
        {
            SNIPacket packet = obj as SNIPacket;

            if (packet != null)
            {
                return Equals(packet);
            }

            return false;
        }

        /// <summary>
        /// Check packet equality
        /// </summary>
        /// <param name="obj"></param>
        /// <returns>true if equal</returns>
        public bool Equals(SNIPacket packet)
        {
            if (packet != null)
            {
                return object.ReferenceEquals(packet, this);
            }

            return false;
        }
    }

    internal class SNIPacketFactory : IDisposable
    {
        private static SNIPacketFactory instance = new SNIPacketFactory();

        public static SNIPacketFactory Instance
        {
            get
            {
                return instance;
            }
        }

        private SNIPacketCache _sniPacketCache;
        private ByteArrayCache _sniPacketBufferCache;

        private SNIPacketFactory() { }

        public void Dispose()
        {
            if(_sniPacketCache != null)
            {
                _sniPacketCache.Dispose();
            }

            if (_sniPacketBufferCache != null)
            {
                _sniPacketBufferCache.Dispose();
            }
        }

        public SNIPacket GetSNIPacket()
        {
            SNIPacket sniPacket = null;

            if (_sniPacketCache != null)
            {
                sniPacket = _sniPacketCache.Get();
            }

            if (sniPacket == null)
            {
                sniPacket = new SNIPacket();
            }

            return sniPacket;
        }

        public SNIPacket GetSNIPacket(int bufferSize)
        {
            SNIPacket sniPacket = GetSNIPacket();
            sniPacket.Data = GetSNIPacketBuffer(bufferSize);

            return sniPacket;
        }

        public void PutSNIPacket(SNIPacket sniPacket)
        {
            if (sniPacket != null)
            {
                if (sniPacket.Data != null)
                {
                    if (_sniPacketBufferCache == null)
                    {
                        _sniPacketBufferCache = new ByteArrayCache();
                    }
                    _sniPacketBufferCache.Put(sniPacket.Data);
                    sniPacket.Data = null;
                }

                sniPacket.Reset();

                if(_sniPacketCache == null)
                {
                    _sniPacketCache = new SNIPacketCache();
                }
                _sniPacketCache.Put(sniPacket);
            }
        }

        public byte[] GetSNIPacketBuffer(int bufferSize)
        {
            byte[] buffer = null;
            if (_sniPacketBufferCache != null)
            {
                buffer = _sniPacketBufferCache.Get(bufferSize);
            }

            if (buffer == null)
            {
                buffer = new byte[bufferSize];
            }

            return buffer;
        }
    }

    internal class SNIPacketCache : IDisposable
    {
        private const int maxSize = 1000;
        private ConcurrentStack<SNIPacket> cache = new ConcurrentStack<SNIPacket>();

        public void Dispose()
        {
            cache.Clear();
        }

        public void Put(SNIPacket sniPacket)
        {
            if (sniPacket == null)
            {
                return;
            }

            if (cache.Count < maxSize)
            {
                cache.Push(sniPacket);
                //Console.WriteLine("count: " + cache.Count);
            }
        }

        public SNIPacket Get()
        {
            SNIPacket sniPacket = null;
            cache.TryPop(out sniPacket);

            return sniPacket;
        }
    }

    internal class ByteArrayCache : IDisposable
    {
        private const int maxSize = 1000;
        private ConcurrentDictionary<int, ConcurrentStack<byte[]>> cacheGroup = new ConcurrentDictionary<int, ConcurrentStack<byte[]>>();

        public void Dispose()
        {
            foreach (ConcurrentStack<byte[]> cs in cacheGroup.Values)
            {
                cs.Clear();
            }
            cacheGroup.Clear();
        }

        public void Put(byte[] buffer)
        {
            if (buffer == null)
            {
                return;
            }

            int bufferSize = buffer.Length;

            if (bufferSize == 0)
            {
                return;
            }

            ConcurrentStack<byte[]> cache = cacheGroup.GetOrAdd(bufferSize, new ConcurrentStack<byte[]>());
            if (cache.Count < maxSize)
            {
                cache.Push(buffer);
            }
        }

        public byte[] Get(int bufferSize)
        {
            if (bufferSize <= 0)
            {
                return null;
            }

            ConcurrentStack<byte[]> cache;
            byte[] buffer = null;
            if (cacheGroup.TryGetValue(bufferSize, out cache))
            {
                cache.TryPop(out buffer);
            }

            return buffer;
        }
    }
}
