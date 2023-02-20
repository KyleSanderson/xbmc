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

CFileCache::CFileCache(const unsigned int flags)
  : CThread("FileCache"),
    m_seekPossible(0),
    m_nSeekResult(0),
    m_seekPos(0),
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
  m_writeLatency.Reset();

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

  CLog::Log(LOGINFO, "CFileCache::{} - <{}> allocated read buffer: {}", __FUNCTION__,
    m_sourcePath, m_chunkSize);

  while (!m_bStop)
  {
    // Update filesize
    m_fileSize = m_source.GetLength();

    /* Super dumb that the first param does nothing with 0ms. */
    m_bFilling = m_pCache->WaitForData(0, 0ms) < m_readCache;
    if (m_seekEvent.Wait(0ms)) // check for seek events
    {
      m_seekEvent.Reset();

      if (m_seekPossible == 0) {
        if (m_pCache->CachedDataEndPosIfSeekTo(m_seekPos) == m_seekPos) {
          m_pCache->Reset(m_seekPos);
          m_nSeekResult = m_seekPos;
        } else if (m_seekPos > m_pCache->CachedDataEndPos()) {
          m_pCache->Reset(m_pCache->CachedDataEndPos());
          m_nSeekResult = m_pCache->CachedDataEndPos();
        } else if (m_seekPos < m_pCache->CachedDataStartPos()) {
          m_pCache->Reset(m_pCache->CachedDataStartPos());
          m_nSeekResult = m_pCache->CachedDataStartPos();
        } else {
          m_nSeekResult = -1;
        }
      } else {
        if (m_pCache->Reset(m_seekPos)) {
          CLog::Log(LOGINFO,
            "CFileCache::{} - <{}> cache completely reset to seek to position {}",
            __FUNCTION__, m_sourcePath, m_pCache->CachedDataEndPos());
        }

        m_nSeekResult = m_source.Seek(m_pCache->CachedDataEndPos(), SEEK_SET);
        if (m_nSeekResult != m_pCache->CachedDataEndPos()) {
          CLog::Log(LOGINFO,
            "CFileCache::{} - <{}> seek failed to position {}",
             __FUNCTION__, m_sourcePath, m_seekPos);
          m_pCache->Reset(m_source.Seek(0, SEEK_CUR));
          m_seekPossible = m_source.IoControl(IOCTRL_SEEK_POSSIBLE, NULL);
        }
      }

      m_seekEnded.Set();
      continue;
    } else if (m_fileSize == m_pCache->CachedDataEndPos()) {
      m_pCache->EndOfInput();

      // The thread event will now also cause the wait of an event to return a false.
      if (AbortableWait(m_seekEvent) == WAIT_SIGNALED)
      {
        m_pCache->ClearEndOfInput();
        if (!m_bStop)
          m_seekEvent.Set(); // hack so that later we realize seek is needed
        continue;
      }

      break; // while (!m_bStop)
    } else if (m_bFilling) {
      m_readEvent.Reset();
    } else if (m_readEvent.Wait(500ms)) {
      m_readEvent.Reset();
      continue;
    } else {
      continue;
    }

    int64_t maxSourceRead = m_chunkSize;
    // Cap source read size by space available between current write position and EOF
    if (m_fileSize != 0)
      maxSourceRead = std::min(maxSourceRead, m_fileSize - m_pCache->CachedDataEndPos());

    maxSourceRead = m_pCache->GetMaxWriteSize(maxSourceRead);
    /* Only read from source if there's enough write space in the cache
    * else we may keep disposing data and seeking back on (slow) source
    */
    if (maxSourceRead < 1) {
      continue;
    }

    auto wtm = m_writeLatency.Begin();
    ssize_t iRead = m_source.Read(buffer.get(), maxSourceRead);
    m_writeLatency.MeasureLatency(wtm);
  
    if (iRead <= 0)
    {
      // Check for actual EOF and retry as long as we still have data in our cache
      if (m_pCache->CachedDataEndPos() < m_fileSize && m_pCache->WaitForData(0, 0ms) > 0)
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
        else if (m_pCache->CachedDataEndPos() < m_fileSize)
          CLog::Log(LOGERROR,
                    "CFileCache::{} - <{}> source read didn't return any data before eof!",
                    __FUNCTION__, m_sourcePath);
        else
          CLog::Log(LOGDEBUG, "CFileCache::{} - <{}> source read hit eof", __FUNCTION__,
                    m_sourcePath);
      }
    }

    int iTotalWrite = 0;
    while (!m_bStop && (iTotalWrite < iRead))
    {
      int iWrite = m_pCache->WriteToCache(buffer.get() + iTotalWrite, iRead - iTotalWrite);

      // write should always work. all handling of buffering and errors should be
      // done inside the cache strategy. only if unrecoverable error happened, WriteToCache would return error and we break.
      if (iWrite < 0)
      {
        CLog::Log(LOGERROR, "CFileCache::{} - <{}> error writing to cache", __FUNCTION__,
                  m_sourcePath);
        m_bStop = true;
        break;
      } else if (iWrite == 0 && m_readEvent.Wait(1ms)) {
        CLog::Log(LOGERROR, "CFileCache::{} - <{}> LATENCY writing to cache remaining: {}/{}", __FUNCTION__,
          m_sourcePath, iTotalWrite, iRead);
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
  m_readEvent.Set();
  // attempt to read
  iRc = m_pCache->ReadFromCache((char *)lpBuf, uiBufSize);
  if (iRc > 0)
  {
    if (iRc > m_readRateMax) {
      m_readRateMax = iRc;
    }

    uint64_t readRate = uiBufSize;

    auto peak = m_writeLatency.GetPeak();
    if (peak) {
      readRate *= peak;
      auto avg = m_writeLatency.GetAverage();
      if (avg > 9) {
        readRate *= (avg / 10);
      }
    } else {
      readRate *= 500*5; /* 500ms * 50ms average = 2500ms. */
    }

    if (m_readRateMax - m_readRate != 0) {
      auto mut = m_readRate;
      if (!mut) {
        mut = 1;
      }

      readRate *= (m_readRateMax / mut);
    }

    m_readRate += iRc;
    m_readRate /= 2;

    if (readRate > static_cast<uint64_t>(m_forwardCacheSize)) {
      readRate = m_forwardCacheSize;
    }

    if (readRate < static_cast<uint64_t>(m_pCache->WaitForData(0, 0ms))) {
      readRate = m_pCache->WaitForData(0, 0ms);
    }

    if (readRate < m_chunkSize*2) {
      readRate = m_chunkSize*2;
    }

    m_readCache = readRate;
    m_readEvent.Set();
    return (int)iRc;
  }

  if (iRc == CACHE_RC_WOULD_BLOCK)
  {
    CLog::Log(LOGDEBUG, "CFileCache::{} - <{}> Ran Dry max: {}ms min: {}ms readrate: {}/{} data: {}", __FUNCTION__,
      m_sourcePath, m_writeLatency.GetPeak(), m_writeLatency.GetAverage(), m_readCache, m_forwardCacheSize, m_pCache->WaitForData(0, 0ms));

    m_readCache = m_forwardCacheSize;
    // just wait for some data to show up
    iRc = m_pCache->WaitForData(1, 10s);
    CLog::Log(LOGDEBUG, "CFileCache::{} - <{}> Ran Dry now available: {}", __FUNCTION__,
      m_sourcePath, iRc);

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

  int64_t iTarget = iFilePosition;
  if (iWhence == SEEK_END)
    iTarget = m_fileSize + iTarget;
  else if (iWhence == SEEK_CUR)
    iTarget = m_pCache->CachedDataStartPos() + iTarget;
  else if (iWhence != SEEK_SET)
    return -1;

  if (iTarget == m_pCache->CachedDataStartPos()) {
    return iTarget;
  }

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

  if (m_nSeekResult < 0) {
     CLog::Log(LOGINFO, "CFileCache::{} - <{}> bad seek result {}", __FUNCTION__,
              m_sourcePath, m_nSeekResult);
    return m_nSeekResult;
  }

  if (m_seekPossible && m_nSeekResult < iTarget)
  {
    CLog::Log(LOGDEBUG, "CFileCache::{} - <{}> waiting for position {}", __FUNCTION__,
              m_sourcePath, iTarget);
    if (m_pCache->WaitForData(static_cast<uint32_t>(iTarget - m_nSeekResult), 10s) <
        iTarget - m_nSeekResult)
    {
      CLog::Log(LOGWARNING, "CFileCache::{} - <{}> failed to get remaining data", __FUNCTION__,
                m_sourcePath);
      return m_nSeekResult;
    }
  } else if (!m_seekPossible) {
    /* could be hours out... no point in having them suffer. */
    return m_nSeekResult;
  }

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
  return m_pCache->CachedDataStartPos();
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
    
    if (m_readRateMax > 0) {
      status->maxrate = m_readRateMax;
    } else {
      status->maxrate = 1024*1024;
    }
    if (m_readRate > 0) {
      status->currate = m_readRate;
    } else {
      status->currate = 512*1024;
    }
    if (m_readRate > 0) {
      status->lowrate = 0;
    } else {
      status->lowrate = 0;
    }

    return 0;
  }

  if (request == IOCTRL_CACHE_SETRATE)
  {
    m_readRate = *static_cast<uint32_t*>(param);
    return 0;
  }

  if (request == IOCTRL_SEEK_POSSIBLE)
    return m_seekPossible;

  return -1;
}
