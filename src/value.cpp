/*
Copyright (c) 2003, 2011, Oracle and/or its affiliates. All rights
reserved.

This program is free software; you can redistribute it and/or
modify it under the terms of the GNU General Public License
as published by the Free Software Foundation; version 2 of
the License.

This program is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
GNU General Public License for more details.

You should have received a copy of the GNU General Public License
along with this program; if not, write to the Free Software
Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA
02110-1301  USA
*/
#include "value.h"
#include "binlog_event.h"
#include <boost/lexical_cast.hpp>
#include <iomanip>
#include <boost/format.hpp>
#include <sys/time.h>

#define DIG_PER_DEC1 9

#define DATETIME_MAX_DECIMALS 6
#define MAX_DATE_STRING_REP_LENGTH 30

// snip from mysql/extra/yassl/include/yassl_types.hpp
typedef unsigned char  uchar;
typedef unsigned char  uint8;
typedef unsigned short uint16;
typedef unsigned int   uint32;
typedef uint8          uint24[3];
typedef uint32         uint64[2];
typedef uint8  opaque;
typedef opaque byte;
typedef unsigned int uint;

// original
typedef short int16;
typedef int int32;
typedef long long longlong;
typedef unsigned long long ulonglong;
typedef unsigned long ulong;
typedef unsigned long long ulonglong;

// snip from mysql/include/my_global.h
#ifndef LL
#ifdef HAVE_LONG_LONG
#define LL(A) A ## LL
#else
#define LL(A) A ## L
#endif
#endif

#ifndef ULL
#ifdef HAVE_LONG_LONG
#define ULL(A) A ## ULL
#else
#define ULL(A) A ## UL
#endif
#endif

// snip from mysql/sql-common/my_time.h
#define mi_sint1korr(A) ((int8)(*A))
#define mi_uint1korr(A) ((uint8)(*A))

#define mi_sint2korr(A) ((int16) (((int16) (((uchar*) (A))[1])) +\
                                  ((int16) ((int16) ((char*) (A))[0]) << 8)))
#define mi_sint3korr(A) ((int32) (((((uchar*) (A))[0]) & 128) ? \
                                  (((uint32) 255L << 24) | \
                                   (((uint32) ((uchar*) (A))[0]) << 16) |\
                                   (((uint32) ((uchar*) (A))[1]) << 8) | \
                                   ((uint32) ((uchar*) (A))[2])) : \
                                  (((uint32) ((uchar*) (A))[0]) << 16) |\
                                  (((uint32) ((uchar*) (A))[1]) << 8) | \
                                  ((uint32) ((uchar*) (A))[2])))
#define mi_sint4korr(A) ((int32) (((int32) (((uchar*) (A))[3])) +\
                                  ((int32) (((uchar*) (A))[2]) << 8) +\
                                  ((int32) (((uchar*) (A))[1]) << 16) +\
                                  ((int32) ((int16) ((char*) (A))[0]) << 24)))
#define mi_sint8korr(A) ((longlong) mi_uint8korr(A))
#define mi_uint2korr(A) ((uint16) (((uint16) (((uchar*) (A))[1])) +\
                                   ((uint16) (((uchar*) (A))[0]) << 8)))
#define mi_uint3korr(A) ((uint32) (((uint32) (((uchar*) (A))[2])) +\
                                   (((uint32) (((uchar*) (A))[1])) << 8) +\
                                   (((uint32) (((uchar*) (A))[0])) << 16)))
#define mi_uint4korr(A) ((uint32) (((uint32) (((uchar*) (A))[3])) +\
                                   (((uint32) (((uchar*) (A))[2])) << 8) +\
                                   (((uint32) (((uchar*) (A))[1])) << 16) +\
                                   (((uint32) (((uchar*) (A))[0])) << 24)))
#define mi_uint5korr(A) ((ulonglong)(((uint32) (((uchar*) (A))[4])) +\
                                    (((uint32) (((uchar*) (A))[3])) << 8) +\
                                    (((uint32) (((uchar*) (A))[2])) << 16) +\
                                    (((uint32) (((uchar*) (A))[1])) << 24)) +\
                                    (((ulonglong) (((uchar*) (A))[0])) << 32))
#define mi_uint6korr(A) ((ulonglong)(((uint32) (((uchar*) (A))[5])) +\
                                    (((uint32) (((uchar*) (A))[4])) << 8) +\
                                    (((uint32) (((uchar*) (A))[3])) << 16) +\
                                    (((uint32) (((uchar*) (A))[2])) << 24)) +\
                        (((ulonglong) (((uint32) (((uchar*) (A))[1])) +\
                                    (((uint32) (((uchar*) (A))[0]) << 8)))) <<\
                                     32))
#define mi_uint7korr(A) ((ulonglong)(((uint32) (((uchar*) (A))[6])) +\
                                    (((uint32) (((uchar*) (A))[5])) << 8) +\
                                    (((uint32) (((uchar*) (A))[4])) << 16) +\
                                    (((uint32) (((uchar*) (A))[3])) << 24)) +\
                        (((ulonglong) (((uint32) (((uchar*) (A))[2])) +\
                                    (((uint32) (((uchar*) (A))[1])) << 8) +\
                                    (((uint32) (((uchar*) (A))[0])) << 16))) <<\
                                     32))
#define mi_uint8korr(A) ((ulonglong)(((uint32) (((uchar*) (A))[7])) +\
                                    (((uint32) (((uchar*) (A))[6])) << 8) +\
                                    (((uint32) (((uchar*) (A))[5])) << 16) +\
                                    (((uint32) (((uchar*) (A))[4])) << 24)) +\
                        (((ulonglong) (((uint32) (((uchar*) (A))[3])) +\
                                    (((uint32) (((uchar*) (A))[2])) << 8) +\
                                    (((uint32) (((uchar*) (A))[1])) << 16) +\
                                    (((uint32) (((uchar*) (A))[0])) << 24))) <<\
                                    32))

#define MY_PACKED_TIME_GET_INT_PART(x)     ((x) >> 24)
#define MY_PACKED_TIME_GET_FRAC_PART(x)    ((x) % (1LL << 24))
#define MY_PACKED_TIME_MAKE(i, f)          ((((longlong) (i)) << 24) + (f))
#define MY_PACKED_TIME_MAKE_INT(i)         ((((longlong) (i)) << 24))



// snip from mysql/include/my_global.h
typedef char		my_bool; /* Small bool */

// snip from mysql/include/mysql_time.h
enum enum_mysql_timestamp_type
{
  MYSQL_TIMESTAMP_NONE= -2, MYSQL_TIMESTAMP_ERROR= -1,
  MYSQL_TIMESTAMP_DATE= 0, MYSQL_TIMESTAMP_DATETIME= 1, MYSQL_TIMESTAMP_TIME= 2
};


typedef struct st_mysql_time
{
  unsigned int  year, month, day, hour, minute, second;
  unsigned long second_part;  /**< microseconds */
  my_bool       neg;
  enum enum_mysql_timestamp_type time_type;
} MYSQL_TIME;


// snip from mysql/sql-common/my_time.c
#define DATETIMEF_INT_OFS 0x8000000000LL

#define TIMEF_OFS 0x800000000000LL
#define TIMEF_INT_OFS 0x800000LL

ulonglong log_10_int[20]=
{
  1, 10, 100, 1000, 10000UL, 100000UL, 1000000UL, 10000000UL,
  ULL(100000000), ULL(1000000000), ULL(10000000000), ULL(100000000000),
  ULL(1000000000000), ULL(10000000000000), ULL(100000000000000),
  ULL(1000000000000000), ULL(10000000000000000), ULL(100000000000000000),
  ULL(1000000000000000000), ULL(10000000000000000000)
};

longlong my_datetime_packed_from_binary(const uchar *ptr, uint dec)
{
  longlong intpart= mi_uint5korr(ptr) - DATETIMEF_INT_OFS;
  int frac;
  switch (dec)
  {
  case 0:
  default:
    return MY_PACKED_TIME_MAKE_INT(intpart);
  case 1:
  case 2:
    frac= ((int) (signed char) ptr[5]) * 10000;
    break;
  case 3:
  case 4:
    frac= mi_sint2korr(ptr + 5) * 100;
    break;
  case 5:
  case 6:
    frac= mi_sint3korr(ptr + 5);
    break;
  }
  return MY_PACKED_TIME_MAKE(intpart, frac);
}

longlong my_time_packed_from_binary(const uchar *ptr, uint dec)
{
  //DBUG_ASSERT(dec <= DATETIME_MAX_DECIMALS);

  switch (dec)
  {
  case 0:
  default:
    {
      longlong intpart= mi_uint3korr(ptr) - TIMEF_INT_OFS;
      return MY_PACKED_TIME_MAKE_INT(intpart);
    }
  case 1:
  case 2:
    {
      longlong intpart= mi_uint3korr(ptr) - TIMEF_INT_OFS;
      int frac= (uint) ptr[3];
      if (intpart < 0 && frac)
      {
        /*
          Negative values are stored with reverse fractional part order,
          for binary sort compatibility.

            Disk value  intpart frac   Time value   Memory value
            800000.00    0      0      00:00:00.00  0000000000.000000
            7FFFFF.FF   -1      255   -00:00:00.01  FFFFFFFFFF.FFD8F0
            7FFFFF.9D   -1      99    -00:00:00.99  FFFFFFFFFF.F0E4D0
            7FFFFF.00   -1      0     -00:00:01.00  FFFFFFFFFF.000000
            7FFFFE.FF   -1      255   -00:00:01.01  FFFFFFFFFE.FFD8F0
            7FFFFE.F6   -2      246   -00:00:01.10  FFFFFFFFFE.FE7960

            Formula to convert fractional part from disk format
            (now stored in "frac" variable) to absolute value: "0x100 - frac".
            To reconstruct in-memory value, we shift
            to the next integer value and then substruct fractional part.
        */
        intpart++;    /* Shift to the next integer value */
        frac-= 0x100; /* -(0x100 - frac) */
      }
      return MY_PACKED_TIME_MAKE(intpart, frac * 10000);
    }

  case 3:
  case 4:
    {
      longlong intpart= mi_uint3korr(ptr) - TIMEF_INT_OFS;
      int frac= mi_uint2korr(ptr + 3);
      if (intpart < 0 && frac)
      {
        /*
          Fix reverse fractional part order: "0x10000 - frac".
          See comments for FSP=1 and FSP=2 above.
        */
        intpart++;      /* Shift to the next integer value */
        frac-= 0x10000; /* -(0x10000-frac) */
      }
      return MY_PACKED_TIME_MAKE(intpart, frac * 100);
    }

  case 5:
  case 6:
    return ((longlong) mi_uint6korr(ptr)) - TIMEF_OFS;
  }
}

using namespace mysql;
using namespace mysql::system;
namespace mysql {

static const int dig2bytes[DIG_PER_DEC1 + 1] = {0, 1, 1, 2, 2, 3, 3, 4, 4, 4};

int decimal_bin_size(int precision, int scale)
{
  int intg   = precision - scale;
  int intg0  = intg / DIG_PER_DEC1;
  int frac0  = scale / DIG_PER_DEC1;
  int intg0x = intg - intg0 * DIG_PER_DEC1;
  int frac0x = scale - frac0 * DIG_PER_DEC1;

  return(
    intg0 * sizeof(boost::int32_t) + dig2bytes[intg0x] +
    frac0 * sizeof(boost::int32_t) + dig2bytes[frac0x]
    );
}

boost::uint32_t my_timestamp_binary_length(boost::uint32_t dec)
{
  return 4 + (dec + 1) / 2;
}

boost::uint32_t my_datetime_binary_length(boost::uint32_t dec)
{
  return 5 + (dec + 1) / 2;
}

boost::uint32_t my_time_binary_length(boost::uint32_t dec)
{
  return 3 + (dec + 1) / 2;
}

static inline int
my_useconds_to_str(char *to, ulong useconds, uint dec)
{
  return sprintf(to, ".%0*lu", (int) dec,
                 useconds / (ulong) log_10_int[DATETIME_MAX_DECIMALS - dec]);
}

int my_time_to_str(const MYSQL_TIME *l_time, char *to, uint dec)
{
  uint extra_hours= 0;
  int len= sprintf(to, "%s%02u:%02u:%02u", (l_time->neg ? "-" : ""),
                   extra_hours + l_time->hour, l_time->minute, l_time->second);
  if (dec)
    len+= my_useconds_to_str(to + len, l_time->second_part, dec);
  return len;
}

int my_date_to_str(const MYSQL_TIME *l_time, char *to)
{
  return sprintf(to, "%04u-%02u-%02u",
                 l_time->year, l_time->month, l_time->day);
}

int my_timeval_to_str(const struct timeval *tm, char *to, uint dec)
{
  int len= sprintf(to, "%d", (int) tm->tv_sec);
  if (dec)
    len+= my_useconds_to_str(to + len, tm->tv_usec, dec);
  return len;
}

void my_timestamp_from_binary(struct timeval *tm, const char* ptr, boost::uint32_t dec)
{
  tm->tv_sec= mi_uint4korr(ptr);
  switch (dec)
  {
    case 0:
    default:
      tm->tv_usec= 0;
      break;
    case 1:
    case 2:
      tm->tv_usec= ((int) ptr[4]) * 10000;
      break;
    case 3:
    case 4:
      tm->tv_usec= mi_sint2korr(ptr + 4) * 100;
      break;
    case 5:
    case 6:
      tm->tv_usec= mi_sint3korr(ptr + 4);
  }
}

static inline int
TIME_to_datetime_str(char *to, const MYSQL_TIME *ltime)
{
  uint32 temp, temp2;
  /* Year */
  temp= ltime->year / 100;
  *to++= (char) ('0' + temp / 10);
  *to++= (char) ('0' + temp % 10);
  temp= ltime->year % 100;
  *to++= (char) ('0' + temp / 10);
  *to++= (char) ('0' + temp % 10);
  *to++= '-';
  /* Month */
  temp= ltime->month;
  temp2= temp / 10;
  temp= temp-temp2 * 10;
  *to++= (char) ('0' + (char) (temp2));
  *to++= (char) ('0' + (char) (temp));
  *to++= '-';
  /* Day */ 
  temp= ltime->day;
  temp2= temp / 10;
  temp= temp - temp2 * 10;
  *to++= (char) ('0' + (char) (temp2));
  *to++= (char) ('0' + (char) (temp));
  *to++= ' ';
  /* Hour */
  temp= ltime->hour;
  temp2= temp / 10;
  temp= temp - temp2 * 10;
  *to++= (char) ('0' + (char) (temp2));
  *to++= (char) ('0' + (char) (temp));
  *to++= ':';
  /* Minute */
  temp= ltime->minute;
  temp2= temp / 10;
  temp= temp - temp2 * 10;
  *to++= (char) ('0' + (char) (temp2));
  *to++= (char) ('0' + (char) (temp));
  *to++= ':';
  /* Second */
  temp= ltime->second;
  temp2=temp / 10;
  temp= temp - temp2 * 10;
  *to++= (char) ('0' + (char) (temp2));
  *to++= (char) ('0' + (char) (temp));
  return 19;
}

int my_datetime_to_str(const MYSQL_TIME *l_time, char *to, uint dec)
{
  int len= TIME_to_datetime_str(to, l_time);
  if (dec)
    len+= my_useconds_to_str(to + len, l_time->second_part, dec);
  else
    to[len]= '\0';
  return len;
}

void TIME_from_longlong_datetime_packed(MYSQL_TIME *ltime, longlong tmp)
{
  longlong ymd, hms;
  longlong ymdhms, ym;
  if ((ltime->neg= (tmp < 0)))
    tmp= -tmp;

  ltime->second_part= MY_PACKED_TIME_GET_FRAC_PART(tmp);
  ymdhms= MY_PACKED_TIME_GET_INT_PART(tmp);

  ymd= ymdhms >> 17;
  ym= ymd >> 5;
  hms= ymdhms % (1 << 17);

  ltime->day= ymd % (1 << 5);
  ltime->month= ym % 13;
  ltime->year= ym / 13;

  ltime->second= hms % (1 << 6);
  ltime->minute= (hms >> 6) % (1 << 6);
  ltime->hour= (hms >> 12);

  ltime->time_type= MYSQL_TIMESTAMP_DATETIME;
}

void TIME_from_longlong_time_packed(MYSQL_TIME *ltime, longlong tmp)
{
  long hms;
  if ((ltime->neg= (tmp < 0)))
    tmp= -tmp;
  hms= MY_PACKED_TIME_GET_INT_PART(tmp);
  ltime->year=   (uint) 0;
  ltime->month=  (uint) 0;
  ltime->day=    (uint) 0;
  ltime->hour=   (uint) (hms >> 12) % (1 << 10); /* 10 bits starting at 12th */
  ltime->minute= (uint) (hms >> 6)  % (1 << 6);  /* 6 bits starting at 6th   */
  ltime->second= (uint)  hms        % (1 << 6);  /* 6 bits starting at 0th   */
  ltime->second_part= MY_PACKED_TIME_GET_FRAC_PART(tmp);
  ltime->time_type= MYSQL_TIMESTAMP_TIME;
}

int string_column_size(boost::uint32_t metadata)
{
  unsigned int byte0 = metadata & 0xFF;
  unsigned int byte1 = metadata >> 8;
  unsigned int col_size;
  if (byte0 != 0)
  {
    if ((byte0 & 0x30) != 0x30)
    {
      /* a long CHAR() field: see #37426 */
      col_size = byte1 | (((byte0 & 0x30) ^ 0x30) << 4);
    }
    else
      col_size = byte1;
  }
  else
    col_size = byte1;
  return col_size;
}

int calc_field_size(unsigned char column_type, const unsigned char *field_ptr, boost::uint32_t metadata)
{
  boost::uint32_t length;

  switch (column_type) {
  case mysql::system::MYSQL_TYPE_VAR_STRING:
    /* This type is hijacked for result set types. */
    length= metadata;
    break;
  case mysql::system::MYSQL_TYPE_NEWDECIMAL:
  {
    int precision = (metadata & 0xff);
    int scale = metadata >> 8;
    length = decimal_bin_size(precision, scale);
    break;
  }
  case mysql::system::MYSQL_TYPE_DECIMAL:
  case mysql::system::MYSQL_TYPE_FLOAT:
  case mysql::system::MYSQL_TYPE_DOUBLE:
    length= metadata;
    break;
  /*
    The cases for SET and ENUM are include for completeness, however
    both are mapped to type MYSQL_TYPE_STRING and their real types
    are encoded in the field metadata.
  */
  case mysql::system::MYSQL_TYPE_SET:
  case mysql::system::MYSQL_TYPE_ENUM:
  case mysql::system::MYSQL_TYPE_STRING:
  {
    //unsigned char type= metadata >> 8U;
    unsigned char type = metadata & 0xff;
    if ((type == mysql::system::MYSQL_TYPE_SET) || (type == mysql::system::MYSQL_TYPE_ENUM))
    {
      //length= metadata & 0x00ff;
      length = (metadata & 0xff00) >> 8;
    }
    else
    {
      /*
        We are reading the actual size from the master_data record
        because this field has the actual lengh stored in the first
        byte.
      */
      unsigned int col_size = string_column_size(metadata);

      if (col_size >= 256)
      {
        length = (unsigned int) *(boost::uint16_t*)field_ptr + 2;
      }
      else
      {
        length = (unsigned int) *field_ptr+1;
      }
      //DBUG_ASSERT(length != 0);
    }
    break;
  }
  case mysql::system::MYSQL_TYPE_YEAR:
  case mysql::system::MYSQL_TYPE_TINY:
    length= 1;
    break;
  case mysql::system::MYSQL_TYPE_SHORT:
    length= 2;
    break;
  case mysql::system::MYSQL_TYPE_INT24:
    length= 3;
    break;
  case mysql::system::MYSQL_TYPE_LONG:
    length= 4;
    break;
  case MYSQL_TYPE_LONGLONG:
    length= 8;
    break;
  case mysql::system::MYSQL_TYPE_NULL:
    length= 0;
    break;
  case mysql::system::MYSQL_TYPE_NEWDATE:
    length= 3;
    break;
  case mysql::system::MYSQL_TYPE_DATE:
  case mysql::system::MYSQL_TYPE_TIME:
    length= 3;
    break;
  case mysql::system::MYSQL_TYPE_TIMESTAMP:
    length= 4;
    break;
  case mysql::system::MYSQL_TYPE_DATETIME:
    length= 8;
    break;
  case mysql::system::MYSQL_TYPE_BIT:
  {
    /*
      Decode the size of the bit field from the master.
        from_len is the length in bytes from the master
        from_bit_len is the number of extra bits stored in the master record
      If from_bit_len is not 0, add 1 to the length to account for accurate
      number of bytes needed.
    */
	boost::uint32_t from_len= (metadata >> 8U) & 0x00ff;
	boost::uint32_t from_bit_len= metadata & 0x00ff;
    //DBUG_ASSERT(from_bit_len <= 7);
    length= from_len + ((from_bit_len > 0) ? 1 : 0);
    break;
  }
  case mysql::system::MYSQL_TYPE_TIMESTAMP2:
    //TODO: metadata is not current. always 0.
    length= my_timestamp_binary_length(metadata);
    break;
  case mysql::system::MYSQL_TYPE_DATETIME2:
    length= my_datetime_binary_length(metadata);
    break;
  case mysql::system::MYSQL_TYPE_TIME2:
    length= my_time_binary_length(metadata);
    break;
  case mysql::system::MYSQL_TYPE_VARCHAR:
  {
    length= metadata > 255 ? 2 : 1;
    length+= length == 1 ? (boost::uint32_t) *field_ptr : *((boost::uint16_t *)field_ptr);
    break;
  }
  case mysql::system::MYSQL_TYPE_TINY_BLOB:
  case mysql::system::MYSQL_TYPE_MEDIUM_BLOB:
  case mysql::system::MYSQL_TYPE_LONG_BLOB:
  case mysql::system::MYSQL_TYPE_BLOB:
  case mysql::system::MYSQL_TYPE_GEOMETRY:
  {
     switch (metadata)
    {
      case 1:
        length= 1+ (boost::uint32_t) field_ptr[0];
        break;
      case 2:
        length= 2+ (boost::uint32_t) (*(boost::uint16_t *)(field_ptr) & 0xFFFF);
        break;
      case 3:
        // TODO make platform indep.
        length= 3+ (boost::uint32_t) (long) (*((boost::uint32_t *) (field_ptr)) & 0xFFFFFF);
        break;
      case 4:
        // TODO make platform indep.
        length= 4+ (boost::uint32_t) (long) *((boost::uint32_t *) (field_ptr));
        break;
      default:
        length= 0;
        break;
    }
    break;
  }
  default:
    length= ~(boost::uint32_t) 0;
  }
  return length;
}

/*
Value::Value(Value &val)
{
  m_size= val.length();
  m_storage= val.storage();
  m_type= val.type();
  m_metadata= val.metadata();
  m_is_null= val.is_null();
}
*/

Value::Value(const Value& val)
{
  m_size= val.m_size;
  m_storage= val.m_storage;
  m_type= val.m_type;
  m_metadata= val.m_metadata;
  m_is_null= val.m_is_null;
}

Value &Value::operator=(const Value &val)
{
  m_size= val.m_size;
  m_storage= val.m_storage;
  m_type= val.m_type;
  m_metadata= val.m_metadata;
  m_is_null= val.m_is_null;
  return *this;
}

bool Value::operator==(const Value &val) const
{
  return (m_size == val.m_size) &&
         (m_storage == val.m_storage) &&
         (m_type == val.m_type) &&
         (m_metadata == val.m_metadata);
}

bool Value::operator!=(const Value &val) const
{
  return !operator==(val);
}

char *Value::as_c_str(unsigned long &size) const
{
  if (m_is_null || m_size == 0)
  {
    size= 0;
    return 0;
  }

  int metadata_length = 0;
  if (m_type == mysql::system::MYSQL_TYPE_VARCHAR) {
    metadata_length = m_metadata > 255 ? 2 : 1;
  } else {
    /*
     Length encoded; First byte or two is length of string.
    */
    metadata_length = string_column_size(m_metadata) > 256 ? 2 : 1;
  }

  /*
   Size is length of the character string; not of the entire storage
  */
  size= m_size - metadata_length;

  return const_cast<char *>(m_storage + metadata_length);
}

unsigned char *Value::as_blob(unsigned long &size) const
{
  if (m_is_null || m_size == 0)
  {
    size= 0;
    return 0;
  }

  /*
   Size was calculated during construction of the object and only inludes the
   size of the blob data, not the metadata part which also is stored in the
   storage. For blobs this part can be between 1-4 bytes long.
  */
  size= m_size - m_metadata;

  /*
   Adjust the storage pointer with the size of the metadata.
  */
  return (unsigned char *)(m_storage + m_metadata);
}

boost::int32_t Value::as_int32() const
{
  if (m_is_null)
  {
    return 0;
  }
  boost::uint32_t to_int;
  Protocol_chunk<boost::uint32_t> prot_integer(to_int);

  buffer_source buff(m_storage, m_size);
  buff >> prot_integer;
  return to_int;
}

boost::int8_t Value::as_int8() const
{
  if (m_is_null)
  {
    return 0;
  }
  boost::int8_t to_int;
  Protocol_chunk<boost::int8_t> prot_integer(to_int);

  buffer_source buff(m_storage, m_size);
  buff >> prot_integer;
  return to_int;
}

boost::int16_t Value::as_int16() const
{
  if (m_is_null)
  {
    return 0;
  }
  boost::int16_t to_int;
  Protocol_chunk<boost::int16_t> prot_integer(to_int);

  buffer_source buff(m_storage, m_size);
  buff >> prot_integer;
  return to_int;
}

boost::int32_t Value::as_int24() const
{
  if (m_is_null)
  {
    return 0;
  }
  boost::int32_t to_int;
  char high_byte = (m_storage[2] & 0x80) == 0 ? 0 : -1;

  to_int = static_cast<boost::int32_t >(((high_byte & 0xff) << 24) |
                                       ((m_storage[2] & 0xff) << 16) |
                                       ((m_storage[1] & 0xff) << 8) |
                                        (m_storage[0] & 0xff));
  return to_int;
}

boost::int64_t Value::as_int64() const
{
  if (m_is_null)
  {
    return 0;
  }
  boost::int64_t to_int;
  Protocol_chunk<boost::int64_t> prot_integer(to_int);

  buffer_source buff(m_storage, m_size);
  buff >> prot_integer;
  return to_int;
}

float Value::as_float() const
{
  // TODO
  return *((const float *)storage());
}

double Value::as_double() const
{
  // TODO
  return *((const double *)storage());
}

boost::int64_t read_int8(char *& data)
{
  boost::int64_t value;
  value = static_cast<boost::int8_t >(data[0]);
  data++;

  return value;
}

boost::int64_t read_int16_be(char *& data)
{
  boost::int64_t value;
  value = static_cast<boost::int16_t >(((data[0] & 0xff) << 8) |
                                       (data[1] & 0xff));
  data += 2;
  return value;
}

boost::int64_t read_int24_be(char *& data)
{
  boost::int64_t value;
  char high_byte = (data[0] & 0x80) == 0 ? 0 : -1;
  value = static_cast<boost::int32_t >(((high_byte & 0xff) << 24) |
                                       ((data[0] & 0xff) << 16) |
                                       ((data[1] & 0xff) << 8) |
                                        (data[2] & 0xff));
  data += 3;
  return value;
}

boost::int64_t read_int32_be(char *& data)
{
  boost::int64_t value;
  value = static_cast<boost::int32_t >(((data[0] & 0xff) << 24) |
                                        ((data[1] & 0xff) << 16) |
                                        ((data[2] & 0xff) << 8) |
                                        (data[3] & 0xff));
  data += 4;

  return value;
}

boost::int64_t read_int_be_by_size(int size, char *& data)
{
  boost::int64_t value;

  switch (size) {
    case 1:
      value = read_int8(data);
      break;
    case 2:
      value = read_int16_be(data);
      break;
    case 3:
      value = read_int24_be(data);
      break;
    case 4:
      value = read_int32_be(data);
      break;
    default:
      throw std::length_error("size " +
                              boost::lexical_cast<std::string>(size) +
                              " not implemented");
      break;
  }

  return value;
}

void strip_leading_zeros(const std::string src, std::string& dst)
{
  if (src.length() == 0) {
    dst.erase();
    return;
  }
  size_t not_zero_pos = src.find_first_not_of('0');
  if (not_zero_pos == std::string::npos) {
    // All characters were 0.  Leave the last 0
    not_zero_pos = src.length() - 1;
  }
  dst.assign(src.substr(not_zero_pos));
}

void strip_trailing_zeros(const std::string src, std::string& dst)
{
  if (src.length() == 0) {
    dst.erase();
    return;
  }
  size_t not_zero_pos = src.find_last_not_of('0');
  if (not_zero_pos == std::string::npos) {
    // All characters were 0.  Leave the first 0
    not_zero_pos = 0;
  }
  dst.assign(src.substr(0, not_zero_pos + 1));
}

void convert_newdecimal(std::string &str, const Value &val)
{
  // The following code has been ported from Ruby mysql_binlog gem
  // (https://github.com/jeremycole/mysql_binlog)
  //
  // Read a (new) decimal value. The value is stored as a sequence of signed
  // big-endian integers, each representing up to 9 digits of the integral
  // and fractional parts. The first integer of the integral part and/or the
  // last integer of the fractional part might be compressed (or packed) and
  // are of variable length. The remaining integers (if any) are
  // uncompressed and 32 bits wide.

  boost::uint32_t metadata = val.metadata();
  int precision = (metadata & 0xff);
  int scale = metadata >> 8;

  char* val_storage = new char[val.length()];
  memcpy(val_storage, val.storage(), val.length());
  char* val_ptr = val_storage;

  int digits_per_integer = 9;
  int compressed_bytes[] = {0, 1, 1, 2, 2, 3, 3, 4, 4, 4};
  int integral = (precision - scale);
  int uncomp_integral = integral / digits_per_integer;
  int uncomp_fractional = scale / digits_per_integer;
  int comp_integral = integral - (uncomp_integral * digits_per_integer);
  int comp_fractional = scale - (uncomp_fractional * digits_per_integer);

  // The sign is encoded in the high bit of the first byte/digit. The byte
  // might be part of a larger integer, so apply the optional bit-flipper
  // and push back the byte into the input stream.
  boost::int64_t value = val_ptr[0];
  boost::int64_t mask;
  if ((value & 0x80) != 0) {
    str = "";
    mask = 0;
  } else {
    str = "-";
    mask = -1;
  }

  val_ptr[0] = value ^ 0x80;

  std::ostringstream ss_integral;

  int size = compressed_bytes[comp_integral];
  if (size > 0) {
    value = read_int_be_by_size(size, val_ptr) ^ mask;
    ss_integral << std::setw(comp_integral) << std::setfill('0') << value;
  }

  for (int i = 0; i < uncomp_integral; i++) {
    value = read_int32_be(val_ptr) ^ mask;
    ss_integral << std::setw(digits_per_integer) << std::setfill('0') << value;
  }

  if (ss_integral.str().length() == 0) {
    // There was no integral part.  Put 0.
    ss_integral << 0;
  }

  std::string str_integral;
  strip_leading_zeros(ss_integral.str(), str_integral);

  std::ostringstream ss_fractional;

  for (int i = 0; i < uncomp_fractional; i++) {
    value = read_int32_be(val_ptr) ^ mask;
    ss_fractional << std::setw(digits_per_integer) << std::setfill('0')
                  << value; // zero fill 
  }

  size = compressed_bytes[comp_fractional];
  if (size > 0) {
    value = read_int_be_by_size(size, val_ptr) ^ mask;
    ss_fractional << std::setw(comp_fractional) << std::setfill('0') << value;
  }

  std::string str_fractional;
  str_fractional.assign(ss_fractional.str());

  str.append(str_integral);
  if (str_fractional.length() > 0) {
    str.append(".");
    str.append(str_fractional);
  }

  delete val_storage;
}

void Converter::to(std::string &str, const Value &val) const
{
  if (val.is_null())
  {
    str= "(NULL)";
    return;
  }

  switch(val.type())
  {
    case MYSQL_TYPE_DECIMAL:
      str= "not implemented";
      break;
    case MYSQL_TYPE_TINY:
      str= boost::lexical_cast<std::string>(static_cast<int>(val.as_int8()));
      break;
    case MYSQL_TYPE_SHORT:
      str= boost::lexical_cast<std::string>(val.as_int16());
      break;
    case MYSQL_TYPE_LONG:
      str= boost::lexical_cast<std::string>(val.as_int32());
      break;
    case MYSQL_TYPE_FLOAT:
    {
      str= boost::str(boost::format("%d") % val.as_float());
    }
      break;
    case MYSQL_TYPE_DOUBLE:
      str= boost::str(boost::format("%d") % val.as_double());
      break;
    case MYSQL_TYPE_NULL:
      str= "not implemented";
      break;
    case MYSQL_TYPE_TIMESTAMP:
    {
      // Return '0000-00-00 00:00:00' for '0' timestamp value,
      // to make it general format.
      // '0' will be shown as '0000-00-00 00:00:00' on only mysql,
      // but '0' will be '1970-01-01 00:00:00' on other databases.
      boost::uint32_t val_uint = (boost::uint32_t)val.as_int32();
      if (val_uint == 0) {
        str= "0000-00-00 00:00:00";
      } else {
        str= boost::lexical_cast<std::string>(val_uint);
      }
      //str= boost::lexical_cast<std::string>((boost::uint32_t)val.as_int32());
      break;
    }
    case MYSQL_TYPE_TIMESTAMP2:
    {
      // snip from mysql/sql/log_event#log_event_print_value
      char buf[MAX_DATE_STRING_REP_LENGTH];
      struct timeval tm;
      my_timestamp_from_binary(&tm, val.storage(), val.metadata());
      int buflen= my_timeval_to_str(&tm, buf, val.metadata());
      str= boost::str(boost::format("%s") % buf);
      // Return '0000-00-00 00:00:00' for '0' timestamp value,
      // to make it general format.
      // '0' will be shown as '0000-00-00 00:00:00' on only mysql,
      // but '0' will be '1970-01-01 00:00:00' on other databases.
      if (boost::lexical_cast<float>(str) == 0.0) {
        str= "0000-00-00 00:00:00" + str.substr(1);  // add fraction part
      }
    }
      break;
    case MYSQL_TYPE_LONGLONG:
      str= boost::lexical_cast<std::string>(val.as_int64());
      break;
    case MYSQL_TYPE_INT24:
      str= boost::lexical_cast<std::string>(val.as_int24());
      break;
    case MYSQL_TYPE_DATE:
    {
      const char* val_storage = val.storage();
      unsigned int date_val = (val_storage[0] & 0xff) + ((val_storage[1] & 0xff) << 8) + ((val_storage[2] & 0xff) << 16);
      unsigned int date_year = date_val >> 9;
      date_val -= (date_year << 9);
      unsigned int date_month = date_val >> 5;
      unsigned int date_day = date_val - (date_month << 5);
      str = boost::str(boost::format("%04d-%02d-%02d") % date_year % date_month % date_day);
      break;
    }
    case MYSQL_TYPE_DATETIME:
    {
      boost::uint64_t timestamp= val.as_int64();
      unsigned long d= timestamp / 1000000;
      unsigned long t= timestamp % 1000000;
      std::ostringstream os;

      os << std::setfill('0') << std::setw(4) << d / 10000
         << std::setw(1) << '-'
         << std::setw(2) << (d % 10000) / 100
         << std::setw(1) << '-'
         << std::setw(2) << d % 100
         << std::setw(1) << ' '
         << std::setw(2) << t / 10000
         << std::setw(1) << ':'
         << std::setw(2) << (t % 10000) / 100
         << std::setw(1) << ':'
         << std::setw(2) << t % 100;

      str= os.str();
    }
      break;
    case MYSQL_TYPE_DATETIME2:
    {
      // snip from mysql/sql/log_event#log_event_print_value
      char buf[MAX_DATE_STRING_REP_LENGTH];
      MYSQL_TIME ltime;
      longlong packed= my_datetime_packed_from_binary((uchar *)val.storage(), val.metadata());
      TIME_from_longlong_datetime_packed(&ltime, packed);
      int buflen= my_datetime_to_str(&ltime, buf, val.metadata());
      str= boost::str(boost::format("%s") % buf);
    }
      break;
    case MYSQL_TYPE_TIME:
    {
      const char* val_storage = val.storage();
      unsigned int time_val = (val_storage[0] & 0xff) + ((val_storage[1] & 0xff) << 8) + ((val_storage[2] & 0xff) << 16);
      unsigned int time_sec = time_val % 100;
      time_val -= time_sec;
      unsigned int time_min = (time_val % 10000) / 100;
      unsigned int time_hour = (time_val - time_min) / 10000;
      str = boost::str(boost::format("%02d:%02d:%02d") % time_hour % time_min % time_sec);
      break;
    }
    case MYSQL_TYPE_TIME2:
    {
      char buf[MAX_DATE_STRING_REP_LENGTH];
      MYSQL_TIME ltime;
      longlong packed= my_time_packed_from_binary((uchar *)val.storage(), val.metadata());
      TIME_from_longlong_time_packed(&ltime, packed);
      int buflen= my_time_to_str(&ltime, buf, val.metadata());
      str= boost::str(boost::format("%s") % buf);
      break;
    }
    case MYSQL_TYPE_YEAR:
    {
      const char* val_storage = val.storage();
      unsigned int year_val = (val_storage[0] & 0xff);
      year_val = year_val > 0 ? (year_val + 1900) : 0;
      str = boost::str(boost::format("%04d") % year_val);
      break;
    }
    case MYSQL_TYPE_NEWDATE:
      str= "not implemented";
      break;
    case MYSQL_TYPE_VARCHAR:
    {
      unsigned long size;
      char *ptr= val.as_c_str(size);
      str.append(ptr, size);
    }
      break;
    case MYSQL_TYPE_VAR_STRING:
    {
      str.append(val.storage(), val.length());
    }
    break;
    case MYSQL_TYPE_STRING:
    {
      unsigned char str_type = 0;

      if (val.metadata()) {
        str_type = val.metadata() & 0xff;
      }

      if (str_type == mysql::system::MYSQL_TYPE_SET) {
        const char* val_ptr = val.storage();
        const int val_length = val.length();
        unsigned long set_value = 0;
        // length is 1, 2, 4 or 8
        for ( int i = 0; i < val_length; i++ ) {
          set_value += (static_cast<unsigned long>(val_ptr[i]) & 0xff) << ( 8 * i );
        }
        str = boost::str(boost::format("%u") % set_value);
        break;
      } else if (str_type == mysql::system::MYSQL_TYPE_ENUM) {
        unsigned int val_storage = static_cast<unsigned int>(*val.storage());
        str = boost::str(boost::format("%u") % val_storage);
        break;
      }

      unsigned long size;
      char *ptr= val.as_c_str(size);
      str.append(ptr, size);
    }
      break;
    case MYSQL_TYPE_BIT:
      str= "not implemented";
      break;
    case MYSQL_TYPE_NEWDECIMAL:
      convert_newdecimal(str, val);
      break;
    case MYSQL_TYPE_ENUM:
      str= "not implemented";
      break;
    case MYSQL_TYPE_SET:
      str= "not implemented";
      break;
    case MYSQL_TYPE_TINY_BLOB:
    case MYSQL_TYPE_MEDIUM_BLOB:
    case MYSQL_TYPE_LONG_BLOB:
    case MYSQL_TYPE_BLOB:
    {
      unsigned long size;
      unsigned char *ptr= val.as_blob(size);
      str.append((const char *)ptr, size);
    }
      break;
    case MYSQL_TYPE_GEOMETRY:
      str= "not implemented";
      break;
    default:
      str= "not implemented";
      break;
  }
}

void Converter::to(float &out, const Value &val) const
{
  switch(val.type())
  {
  case MYSQL_TYPE_FLOAT:
    out= val.as_float();
    break;
  default:
    out= 0;
  }
}

void Converter::to(long &out, const Value &val) const
{
  switch(val.type())
  {
    case MYSQL_TYPE_DECIMAL:
      // TODO
      out= 0;
      break;
    case MYSQL_TYPE_TINY:
      out= val.as_int8();
      break;
    case MYSQL_TYPE_SHORT:
      out= val.as_int16();
      break;;
    case MYSQL_TYPE_LONG:
      out= (long)val.as_int32();
      break;
    case MYSQL_TYPE_FLOAT:
      out= 0;
      break;
    case MYSQL_TYPE_DOUBLE:
      out= (long)val.as_double();
    case MYSQL_TYPE_NULL:
      out= 0;
      break;
    case MYSQL_TYPE_TIMESTAMP:
      out=(boost::uint32_t)val.as_int32();
      break;

    case MYSQL_TYPE_LONGLONG:
      out= (long)val.as_int64();
      break;
    case MYSQL_TYPE_INT24:
      out= 0;
      break;
    case MYSQL_TYPE_DATE:
      out= 0;
      break;
    case MYSQL_TYPE_TIME:
      out= 0;
      break;
    case MYSQL_TYPE_DATETIME:
      out= (long)val.as_int64();
      break;
    case MYSQL_TYPE_YEAR:
      out= 0;
      break;
    case MYSQL_TYPE_NEWDATE:
      out= 0;
      break;
    case MYSQL_TYPE_VARCHAR:
      out= 0;
      break;
    case MYSQL_TYPE_BIT:
      out= 0;
      break;
    case MYSQL_TYPE_NEWDECIMAL:
      out= 0;
      break;
    case MYSQL_TYPE_ENUM:
      out= 0;
      break;
    case MYSQL_TYPE_SET:
      out= 0;
      break;
    case MYSQL_TYPE_TINY_BLOB:
    case MYSQL_TYPE_MEDIUM_BLOB:
    case MYSQL_TYPE_LONG_BLOB:
    case MYSQL_TYPE_BLOB:
      out= 0;
      break;
    case MYSQL_TYPE_VAR_STRING:
    {
      std::string str;
      str.append(val.storage(), val.length());
      out= boost::lexical_cast<long>(str.c_str());
    }
      break;
    case MYSQL_TYPE_STRING:
      out= 0;
      break;
    case MYSQL_TYPE_GEOMETRY:
      out= 0;
      break;
    default:
      out= 0;
      break;
  }
}


} // end namespace mysql
