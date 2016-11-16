/* Copyright (c) 2001, 2014, Oracle and/or its affiliates. All rights reserved.

   This program is free software; you can redistribute it and/or modify
   it under the terms of the GNU General Public License as published by
   the Free Software Foundation; version 2 of the License.

   This program is distributed in the hope that it will be useful,
   but WITHOUT ANY WARRANTY; without even the implied warranty of
   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
   GNU General Public License for more details.

   You should have received a copy of the GNU General Public License
   along with this program; if not, write to the Free Software
   Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA  02110-1301  USA */

/*
  Endianness-independent definitions for architectures other
  than the x86 architecture.

  (From include/byte_order_generic.h v5.7.16)
*/

#include <boost/asio.hpp>

static inline boost::int16_t sint2korr(const uchar *A) { return *((boost::int16_t*) A); }

static inline boost::int32_t sint4korr(const uchar *A) { return *((boost::int32_t*) A); }

static inline boost::uint16_t uint2korr(const uchar *A) { return *((boost::uint16_t*) A); }

static inline boost::uint32_t uint4korr(const uchar *A) { return *((boost::uint32_t*) A); }

static inline ulonglong uint8korr(const uchar *A) { return *((ulonglong*) A);}

static inline longlong  sint8korr(const uchar *A) { return *((longlong*) A); }


static inline void int2store(uchar *T, boost::uint16_t A)
{
  *((boost::uint16_t*) T)= A;
}

static inline void int4store(uchar *T, boost::uint32_t A)
{
  *((boost::uint32_t*) T)= A;
}

static inline void int8store(uchar *T, ulonglong A)
{
  *((ulonglong*) T)= A;
}


static inline boost::int16_t sint2korr(const char *pT)
{
  return sint2korr(static_cast<const uchar*>(static_cast<const void*>(pT)));
}

static inline boost::uint16_t uint2korr(const char *pT)
{
  return uint2korr(static_cast<const uchar*>(static_cast<const void*>(pT)));
}

static inline boost::uint32_t uint4korr(const char *pT)
{
  return uint4korr(static_cast<const uchar*>(static_cast<const void*>(pT)));
}

static inline boost::int32_t sint4korr(const char *pT)
{
  return sint4korr(static_cast<const uchar*>(static_cast<const void*>(pT)));
}

static inline ulonglong uint8korr(const char *pT)
{
  return uint8korr(static_cast<const uchar*>(static_cast<const void*>(pT)));
}

static inline longlong  sint8korr(const char *pT)
{
  return sint8korr(static_cast<const uchar*>(static_cast<const void*>(pT)));
}


static inline void int2store(char *pT, boost::uint16_t A)
{
  int2store(static_cast<uchar*>(static_cast<void*>(pT)), A);
}

static inline void int4store(char *pT, boost::uint32_t A)
{
  int4store(static_cast<uchar*>(static_cast<void*>(pT)), A);
}

static inline void int8store(char *pT, ulonglong A)
{
  int8store(static_cast<uchar*>(static_cast<void*>(pT)), A);
}


static inline void float8get  (double *V, const char *M)
{
  memcpy(V, static_cast<const uchar*>(static_cast<const void*>(M)), sizeof(double));
}
