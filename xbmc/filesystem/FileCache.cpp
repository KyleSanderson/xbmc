/*
 *  Copyright (C) 2005-2018 Team Kodi
 *  This file is part of Kodi - https://kodi.tv
 *
 *  SPDX-License-Identifier: GPL-2.0-or-later
 *  See LICENSES/README.md for more information.
 */

#include "FileCache.h"

#include "CircularCache.h"
#include "ServiceBroker.h"
#include "URL.h"
#include "settings/AdvancedSettings.h"
#include "settings/SettingsComponent.h"
#include "threads/Thread.h"
#include "utils/log.h"

#include <mutex>

#if !defined(TARGET_WINDOWS)
#include "platform/posix/ConvUtils.h"
#endif

#include <algorithm>
#include <cassert>
#include <chrono>
#include <inttypes.h>
#include <memory>

#ifdef TARGET_POSIX
#include "platform/posix/ConvUtils.h"
#endif

using namespace XFILE;
using namespace std::chrono_literals;

class CWriteRate
{
public:
  CWriteRate()
  {
    m_stamp = std::chrono::steady_clock::now();
    m_pos   = 0;
    m_size = 0;
    m_time = std::chrono::milliseconds(0);
  }

  void Reset(int64_t pos, bool bResetAll = true)
  {
    m_stamp = std::chrono::steady_clock::now();
    m_pos   = pos;

    if (bResetAll)
    {
      m_size  = 0;
      m_time = std::chrono::milliseconds(0);
    }
  }

  uint32_t Rate(int64_t pos, uint32_t time_bias = 0)
  {
    auto ts = std::chrono::steady_clock::now();

    m_size += (pos - m_pos);
    m_time += std::chrono::duration_cast<std::chrono::milliseconds>(ts - m_stamp);
    m_pos = pos;
    m_stamp = ts;

    if (m_time == std::chrono::milliseconds(0))
      return 0;

    return static_cast<uint32_t>(1000 * (m_size / (m_time.count() + time_bias)));
  }

private:
  std::chrono::time_point<std::chrono::steady_clock> m_stamp;
  int64_t  m_pos;
  std::chrono::milliseconds m_time;
  int64_t  m_size;
};


CFileCache::CFileCache(const unsigned int flags)
  : CThread("FileCache"),
    m_seekPossible(0),
    m_nSeekResult(0),
    m_seekPos(0),
    m_readPos(0),
    m_writePos(0),
    m_chunkSize(0),
    m_writeRate(0),
    m_writeRateActual(0),
    m_writeRateLowSpeed(0),
    m_readRate(0),
    m_readRateMax(0),
    m_readCache(0),
    m_forwardCacheSize(0),
    m_bFilling(false),
    m_fileSize(0),
    m_flags(flags)
{
}

CFileCache::~CFileCache()
{
  Close();
}

IFile *CFileCache::GetFileImp()
{
  return m_source.GetImplementation();
}

bool CFileCache::Open(const CURL& url)
{
  Close();

  std::unique_lock<CCriticalSection> lock(m_sync);

  m_sourcePath = url.GetRedacted();

  CLog::Log(LOGDEBUG, "CFileCache::{} - <{}> opening", __FUNCTION__, m_sourcePath);

  // opening the source file.
  if (!m_source.Open(url.Get(), READ_NO_CACHE | READ_TRUNCATED | READ_CHUNKED))
  {
    CLog::Log(LOGERROR, "CFileCache::{} - <{}> failed to open", __FUNCTION__, m_sourcePath);
    Close();
    return false;
  }

  m_source.IoControl(IOCTRL_SET_CACHE, this);

  bool retry = false;
  m_source.IoControl(IOCTRL_SET_RETRY, &retry); // We already handle retrying ourselves

  // check if source can seek
  m_seekPossible = m_source.IoControl(IOCTRL_SEEK_POSSIBLE, NULL);

  // Determine the best chunk size we can use
  m_chunkSize = CFile::DetermineChunkSize(
      m_source.GetChunkSize(),
      CServiceBroker::GetSettingsComponent()->GetAdvancedSettings()->m_cacheChunkSize);
  CLog::Log(LOGDEBUG,
            "CFileCache::{} - <{}> source chunk size is {}, setting cache chunk size to {}",
            __FUNCTION__, m_sourcePath, m_source.GetChunkSize(), m_chunkSize);

  m_fileSize = m_source.GetLength();

  if (!m_pCache)
  {
    if (CServiceBroker::GetSettingsComponent()->GetAdvancedSettings()->m_cacheMemSize == 0)
    {
      // Use cache on disk
      m_pCache = std::unique_ptr<CSimpleFileCache>(new CSimpleFileCache()); // C++14 - Replace with std::make_unique
      m_forwardCacheSize = 0;
    }
    else
    {
      size_t cacheSize;
      if (m_fileSize > 0 && m_fileSize < CServiceBroker::GetSettingsComponent()->GetAdvancedSettings()->m_cacheMemSize && !(m_flags & READ_AUDIO_VIDEO))
      {
        // Cap cache size by filesize, but not for audio/video files as those may grow.
        // We don't need to take into account READ_MULTI_STREAM here as that's only used for audio/video
        cacheSize = m_fileSize;

        // Cap chunk size by cache size
        if (m_chunkSize > cacheSize)
          m_chunkSize = cacheSize;
      }
      else
      {
        cacheSize = CServiceBroker::GetSettingsComponent()->GetAdvancedSettings()->m_cacheMemSize;

        // NOTE: READ_MULTI_STREAM is only used with READ_AUDIO_VIDEO
        if (m_flags & READ_MULTI_STREAM)
        {
          // READ_MULTI_STREAM requires double buffering, so use half the amount of memory for each buffer
          cacheSize /= 2;
        }

        // Make sure cache can at least hold 2 chunks
        if (cacheSize < m_chunkSize * 2)
          cacheSize = m_chunkSize * 2;
      }

      if (m_flags & READ_MULTI_STREAM)
        CLog::Log(LOGDEBUG, "CFileCache::{} - <{}> using double memory cache each sized {} bytes",
                  __FUNCTION__, m_sourcePath, cacheSize);
      else
        CLog::Log(LOGDEBUG, "CFileCache::{} - <{}> using single memory cache sized {} bytes",
                  __FUNCTION__, m_sourcePath, cacheSize);

      const size_t back = cacheSize / 4;
      const size_t front = cacheSize - back;

      m_pCache = std::unique_ptr<CCircularCache>(new CCircularCache(front, back)); // C++14 - Replace with std::make_unique
      m_forwardCacheSize = front;
    }

    if (m_flags & READ_MULTI_STREAM)
    {
      // If READ_MULTI_STREAM flag is set: Double buffering is required
      m_pCache = std::unique_ptr<CDoubleCache>(new CDoubleCache(m_pCache.release())); // C++14 - Replace with std::make_unique
    }
  }

  // open cache strategy
  if (!m_pCache || m_pCache->Open() != CACHE_RC_OK)
  {
    CLog::Log(LOGERROR, "CFileCache::{} - <{}> failed to open cache", __FUNCTION__, m_sourcePath);
    Close();
    return false;
  }

  m_readPos = 0;
  m_writePos = 0;
  m_writeRate = 1024 * 1024;
  m_writeRateActual = 0;
  m_writeRateLowSpeed = 0;
  m_readRate = 0;
  m_readRateMax = 0;
  m_readCache = m_forwardCacheSize;
  m_bFilling = true;
  m_seekEvent.Reset();
  m_seekEnded.Reset();
  m_readEvent.Reset();

  CThread::Create(false);

  return true;
}

void CFileCache::Process()
{
  if (!m_pCache)
  {
    CLog::Log(LOGERROR, "CFileCache::{} - <{}> sanity failed. no cache strategy", __FUNCTION__,
              m_sourcePath);
    return;
  }

  // create our read buffer
  std::unique_ptr<char[]> buffer(new char[m_chunkSize]);
  if (buffer == nullptr)
  {
    CLog::Log(LOGERROR, "CFileCache::{} - <{}> failed to allocate read buffer", __FUNCTION__,
              m_sourcePath);
    return;
  }

  while (!m_bStop)
  {
     // Update filesize
    m_fileSize = m_source.GetLength();

    /* Super dumb that the first param does nothing with 0ms. */
    m_bFilling = m_pCache->WaitForData(0, 0ms) < m_readCache;
    if (m_seekEvent.Wait(0ms)) // check for seek events
    {
      m_seekEvent.Reset();
      const int64_t cacheMaxPos = m_pCache->CachedDataEndPosIfSeekTo(m_seekPos);
      const bool cacheReachEOF = (cacheMaxPos == m_fileSize);

      bool sourceSeekFailed = false;
      if (!cacheReachEOF)
      {
        m_nSeekResult = m_source.Seek(cacheMaxPos, SEEK_SET);
        if (m_nSeekResult != cacheMaxPos)
        {
          CLog::Log(LOGERROR, "CFileCache::{} - <{}> error {} seeking. Seek returned {}",
                    __FUNCTION__, m_sourcePath, GetLastError(), m_nSeekResult);
          m_seekPossible = m_source.IoControl(IOCTRL_SEEK_POSSIBLE, NULL);
          sourceSeekFailed = true;
        }
      }

      if (!sourceSeekFailed)
      {
        const bool bCompleteReset = m_pCache->Reset(m_seekPos);
        m_readPos = m_seekPos;
        m_writePos = m_pCache->CachedDataEndPos();
        assert(m_writePos == cacheMaxPos);

        m_nSeekResult = m_seekPos;
        if (bCompleteReset)
        {
          CLog::Log(LOGDEBUG,
                    "CFileCache::{} - <{}> cache completely reset for seek to position {}",
                    __FUNCTION__, m_sourcePath, m_seekPos);
          m_writeRateLowSpeed = 0;
        }
      }

      m_seekEnded.Set();
      continue;
    } else if (m_bFilling) {
      m_readEvent.Reset();
    } else if (m_readEvent.Wait(1000ms)) {
      m_readEvent.Reset();
      continue;
    } else {
      continue;
    }

    int64_t maxSourceRead = m_chunkSize;
    // Cap source read size by space available between current write position and EOF
    if (m_fileSize != 0)
      maxSourceRead = std::min(maxSourceRead, m_fileSize - m_writePos);

    const int64_t maxWrite = m_pCache->GetMaxWriteSize(m_chunkSize);
    /* Only read from source if there's enough write space in the cache
    * else we may keep disposing data and seeking back on (slow) source
    */
    if (maxWrite < 1) {
      continue;
    } else if (maxSourceRead > maxWrite)
    {
      maxSourceRead = maxWrite;
    }

    ssize_t iRead = m_source.Read(buffer.get(), maxSourceRead);
    if (!iRead) {
      continue;
    }

    if (iRead < 0)
    {
      // Check for actual EOF and retry as long as we still have data in our cache
      if (m_writePos < m_fileSize && m_pCache->WaitForData(0, 0ms) > 0)
      {
        CLog::Log(LOGWARNING, "CFileCache::{} - <{}> source read returned {}! Will retry",
                  __FUNCTION__, m_sourcePath, iRead);
 
        continue; // while (!m_bStop)
      }
      else
      {
        if (iRead < 0)
          CLog::Log(LOGERROR,
                    "{} - <{}> source read failed with {}!", __FUNCTION__, m_sourcePath, iRead);
        else if (m_fileSize == 0)
          CLog::Log(LOGDEBUG,
                    "CFileCache::{} - <{}> source read didn't return any data! Hit eof(?)",
                    __FUNCTION__, m_sourcePath);
        else if (m_writePos < m_fileSize)
          CLog::Log(LOGERROR,
                    "CFileCache::{} - <{}> source read didn't return any data before eof!",
                    __FUNCTION__, m_sourcePath);
        else
          CLog::Log(LOGDEBUG, "CFileCache::{} - <{}> source read hit eof", __FUNCTION__,
                    m_sourcePath);

        m_pCache->EndOfInput();

        // The thread event will now also cause the wait of an event to return a false.
        if (AbortableWait(m_seekEvent) == WAIT_SIGNALED)
        {
          m_pCache->ClearEndOfInput();
          if (!m_bStop)
            m_seekEvent.Set(); // hack so that later we realize seek is needed
        }
        else
          break; // while (!m_bStop)
      }
    }

    int iTotalWrite = 0;
    while (!m_bStop && (iTotalWrite < iRead))
    {
      int iWrite = 0;
      iWrite = m_pCache->WriteToCache(buffer.get() + iTotalWrite, iRead - iTotalWrite);

      // write should always work. all handling of buffering and errors should be
      // done inside the cache strategy. only if unrecoverable error happened, WriteToCache would return error and we break.
      if (iWrite < 0)
      {
        CLog::Log(LOGERROR, "CFileCache::{} - <{}> error writing to cache", __FUNCTION__,
                  m_sourcePath);
        m_bStop = true;
        break;
      } if (iWrite == 0 && m_readEvent.Wait(1ms)) {
        m_readEvent.Set();
      }

      iTotalWrite += iWrite;

      // check if seek was asked. otherwise if cache is full we'll freeze.
      if (m_seekEvent.Wait(0ms))
      {
        if (!m_bStop)
          m_seekEvent.Set(); // make sure we get the seek event later.
        break;
      }
    }

    m_writePos += iTotalWrite;

    // under estimate write rate by a second, to
    // avoid uncertainty at start of caching
    m_writeRateActual = m_readRate;

   /* NOTE: We can only reliably test for low speed condition, when the cache is *really*
    * filling. This is because as soon as it's full the average-
    * rate will become approximately the current-rate which can flag false
    * low read-rate conditions.
    */
    if (m_forwardCacheSize != 0)
    {
      const int64_t forward = m_pCache->WaitForData(0, 0ms);
      if (forward + m_chunkSize >= m_forwardCacheSize)
      {
        if (m_writeRateActual < m_writeRate)
          m_writeRateLowSpeed = m_writeRateActual;
      }
    }
  }
}

void CFileCache::OnExit()
{
  m_bStop = true;

  // make sure cache is set to mark end of file (read may be waiting).
  if (m_pCache)
    m_pCache->EndOfInput();

  // just in case someone's waiting...
  m_seekEnded.Set();
}

bool CFileCache::Exists(const CURL& url)
{
  return CFile::Exists(url.Get());
}

int CFileCache::Stat(const CURL& url, struct __stat64* buffer)
{
  return CFile::Stat(url.Get(), buffer);
}

ssize_t CFileCache::Read(void* lpBuf, size_t uiBufSize)
{
  std::unique_lock<CCriticalSection> lock(m_sync);
  if (!m_pCache)
  {
    CLog::Log(LOGERROR, "CFileCache::{} - <{}> sanity failed. no cache strategy!", __FUNCTION__,
              m_sourcePath);
    return -1;
  }
  int64_t iRc;

  if (uiBufSize > SSIZE_MAX)
    uiBufSize = SSIZE_MAX;

retry:
  // attempt to read
  iRc = m_pCache->ReadFromCache((char *)lpBuf, uiBufSize);
  if (iRc > 0)
  {
    if (iRc > m_readRateMax) {
      m_readRateMax = iRc;
    }

    float readRatio;
    if (m_readRate == 0) {
      readRatio = 32.0f;
    } else if (iRc > m_readRate*2) {
      readRatio = 16.0f;
    } else if (iRc*2 > m_readRate*3) {
      readRatio = 8.0f;
    } else if (iRc*3 > m_readRate*4) {
      readRatio = 4.0f;
    } else if (iRc*4 > m_readRate*5) {
      readRatio = 2.0f;
     } else {
      readRatio = m_readRateMax / m_readRate;
    }

    m_readRate += iRc;
    m_readRate /= 2;
    m_readCache = m_readRateMax * readRatio * 1024;
    if (m_readCache > m_forwardCacheSize) {
      m_readCache = m_forwardCacheSize;
    }

    m_readPos += iRc;
    m_readEvent.Set();
    return (int)iRc;
  }

  if (iRc == CACHE_RC_WOULD_BLOCK)
  {
    m_readEvent.Set();
    // just wait for some data to show up
    iRc = m_pCache->WaitForData(1, 10s);
    if (iRc > 0)
      goto retry;
  }

  if (iRc == CACHE_RC_TIMEOUT)
  {
    CLog::Log(LOGWARNING, "CFileCache::{} - <{}> timeout waiting for data", __FUNCTION__,
              m_sourcePath);
    return -1;
  }

  if (iRc == 0)
    return 0;

  // unknown error code
  CLog::Log(LOGERROR, "CFileCache::{} - <{}> cache strategy returned unknown error code {}",
            __FUNCTION__, m_sourcePath, (int)iRc);
  return -1;
}

int64_t CFileCache::Seek(int64_t iFilePosition, int iWhence)
{
  std::unique_lock<CCriticalSection> lock(m_sync);

  if (!m_pCache)
  {
    CLog::Log(LOGERROR, "CFileCache::{} - <{}> sanity failed. no cache strategy!", __FUNCTION__,
              m_sourcePath);
    return -1;
  }

  int64_t iCurPos = m_readPos;
  int64_t iTarget = iFilePosition;
  if (iWhence == SEEK_END)
    iTarget = m_fileSize + iTarget;
  else if (iWhence == SEEK_CUR)
    iTarget = iCurPos + iTarget;
  else if (iWhence != SEEK_SET)
    return -1;

  if (iTarget == m_readPos)
    return m_readPos;

  m_readRate = 0;
  if ((m_nSeekResult = m_pCache->Seek(iTarget)) != iTarget)
  {
    if (m_seekPossible == 0)
      return m_nSeekResult;

    // Never request closer to end than one chunk. Speeds up tag reading
    m_seekPos = std::min(iTarget, std::max((int64_t)0, m_fileSize - m_chunkSize));

    m_seekEvent.Set();
    m_readEvent.Set();
    while (!m_seekEnded.Wait(100ms))
    {
      // SeekEnded will never be set if FileCache thread is not running
      if (!CThread::IsRunning())
        return -1;
    }

    /* wait for any remaining data */
    if(m_seekPos < iTarget)
    {
      CLog::Log(LOGDEBUG, "CFileCache::{} - <{}> waiting for position {}", __FUNCTION__,
                m_sourcePath, iTarget);
      if (m_pCache->WaitForData(static_cast<uint32_t>(iTarget - m_seekPos), 10s) <
          iTarget - m_seekPos)
      {
        CLog::Log(LOGWARNING, "CFileCache::{} - <{}> failed to get remaining data", __FUNCTION__,
                  m_sourcePath);
        return -1;
      }
      m_pCache->Seek(iTarget);
    }
    m_readPos = iTarget;
    m_seekEvent.Reset();
  }
  else
    m_readPos = iTarget;

  return iTarget;
}

void CFileCache::Close()
{
  StopThread();

  std::unique_lock<CCriticalSection> lock(m_sync);
  if (m_pCache)
    m_pCache->Close();

  m_source.Close();
}

int64_t CFileCache::GetPosition()
{
  return m_readPos;
}

int64_t CFileCache::GetLength()
{
  return m_fileSize;
}

void CFileCache::StopThread(bool bWait /*= true*/)
{
  m_bStop = true;
  //Process could be waiting for seekEvent
  m_seekEvent.Set();
  CThread::StopThread(bWait);
}

const std::string CFileCache::GetProperty(XFILE::FileProperty type, const std::string &name) const
{
  if (!m_source.GetImplementation())
    return IFile::GetProperty(type, name);

  return m_source.GetImplementation()->GetProperty(type, name);
}

int CFileCache::IoControl(EIoControl request, void* param)
{
  if (request == IOCTRL_CACHE_STATUS)
  {
    SCacheStatus* status = (SCacheStatus*)param;
    status->forward = m_pCache->WaitForData(0, 0ms);
    status->maxrate = m_writeRate;
    status->currate = m_writeRateActual;
    status->lowrate = m_writeRateLowSpeed;
    m_writeRateLowSpeed = 0; // Reset low speed condition
    return 0;
  }

  if (request == IOCTRL_CACHE_SETRATE)
  {
    m_writeRate = *static_cast<uint32_t*>(param);
    return 0;
  }

  if (request == IOCTRL_SEEK_POSSIBLE)
    return m_seekPossible;

  return -1;
}
