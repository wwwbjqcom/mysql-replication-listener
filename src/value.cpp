/*
Copyright (c) 2003, 2011, 2013, Oracle and/or its affiliates. All rights
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
#include <iomanip>
// my_time.h is private header. This file needs to be copied to include/mysql directory manually.
#include <my_time.h>

#define DIG_PER_DEC1 9

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
    intg0 * sizeof(int32_t) + dig2bytes[intg0x] +
    frac0 * sizeof(int32_t) + dig2bytes[frac0x]
    );
}

int string_column_size(uint32_t metadata)
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

uint32_t calc_field_size(unsigned char column_type, const unsigned char *field_ptr,
                    uint32_t metadata)
{
  uint32_t length;

  switch (column_type)
  {
  case MYSQL_TYPE_VAR_STRING:
    /* This type is hijacked for result set types. */
    length= metadata;
    break;
  case MYSQL_TYPE_NEWDECIMAL:
  {
    int precision = (metadata & 0xff);
    int scale = metadata >> 8;
    length = decimal_bin_size(precision, scale);
    break;
  }
  case MYSQL_TYPE_DECIMAL:
  case MYSQL_TYPE_FLOAT:
  case MYSQL_TYPE_DOUBLE:
    length= metadata;
    break;
  /*
    The cases for SET and ENUM are include for completeness, however
    both are mapped to type MYSQL_TYPE_STRING and their real types
    are encoded in the field metadata.
  */
  case MYSQL_TYPE_SET:
  case MYSQL_TYPE_ENUM:
  case MYSQL_TYPE_STRING:
    {
      //unsigned char type= metadata >> 8U;
      unsigned char type = metadata & 0xff;
      if ((type == MYSQL_TYPE_SET) || (type == MYSQL_TYPE_ENUM))
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
          length = (unsigned int) *(uint16_t*)field_ptr + 2;
        }
        else
        {
          length = (unsigned int) *field_ptr+1;
        }
        //DBUG_ASSERT(length != 0);
      }
      break;
    }
  case MYSQL_TYPE_YEAR:
  case MYSQL_TYPE_TINY:
    length= 1;
    break;
  case MYSQL_TYPE_SHORT:
    length= 2;
    break;
  case MYSQL_TYPE_LONG:
    length= 4;
    break;
  case MYSQL_TYPE_LONGLONG:
    length= 8;
    break;
  case MYSQL_TYPE_NULL:
    length= 0;
    break;
  case MYSQL_TYPE_INT24:
  case MYSQL_TYPE_NEWDATE:
  case MYSQL_TYPE_DATE:
  case MYSQL_TYPE_TIME:
    length= 3;
    break;
  case MYSQL_TYPE_TIMESTAMP:
    length= 4;
    break;
  case MYSQL_TYPE_DATETIME:
    length= 8;
    break;
  case MYSQL_TYPE_BIT:
    {
      /*
        Decode the size of the bit field from the master.
        from_len is the length in bytes from the master
        from_bit_len is the number of extra bits stored in the master record
        If from_bit_len is not 0, add 1 to the length to account for accurate
        number of bytes needed.
      */
	    uint32_t from_len= (metadata >> 8U) & 0x00ff;
	    uint32_t from_bit_len= metadata & 0x00ff;
      //DBUG_ASSERT(from_bit_len <= 7);
      length= from_len + ((from_bit_len > 0) ? 1 : 0);
      break;
    }
  case MYSQL_TYPE_VARCHAR:
    {
      length= metadata > 255 ? 2 : 1;
      length+= length == 1 ? (uint32_t) *field_ptr : *((uint16_t *)field_ptr);
      break;
    }
  case MYSQL_TYPE_TINY_BLOB:
  case MYSQL_TYPE_MEDIUM_BLOB:
  case MYSQL_TYPE_LONG_BLOB:
  case MYSQL_TYPE_BLOB:
  case MYSQL_TYPE_GEOMETRY:
    {
      switch (metadata)
      {
        case 1:
          length= 1 + (uint32_t) field_ptr[0];
          break;
        case 2:
          length= 2 + (uint32_t) (*(uint16_t *)(field_ptr) & 0xFFFF);
          break;
        case 3:
          // TODO make platform indep.
          length= 3 + (uint32_t) (long) (*((uint32_t *) (field_ptr)) &
                                                         0xFFFFFF);
          break;
        case 4:
          // TODO make platform indep.
          length= 4 + (uint32_t) (long) *((uint32_t *) (field_ptr));
          break;
        default:
          length= 0;
          break;
      }
      break;
    }
  case MYSQL_TYPE_TIME2:
    length= my_time_binary_length(metadata);
    break;
  case MYSQL_TYPE_TIMESTAMP2:
    //TODO: metadata is not current. always 0.
    length= my_timestamp_binary_length(metadata);
    break;
  case MYSQL_TYPE_DATETIME2:
    length= my_datetime_binary_length(metadata);
    break;
  default:
    length= UINT_MAX;
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
  if (m_type == MYSQL_TYPE_VARCHAR) {
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
  return (unsigned char*)(m_storage + m_metadata);
}

int32_t Value::as_int32() const
{
  if (m_is_null)
  {
    return 0;
  }
  uint32_t to_int;
  Protocol_chunk<uint32_t> prot_integer(to_int);

  buffer_source buff(m_storage, m_size);
  buff >> prot_integer;
  return to_int;
}

int8_t Value::as_int8() const
{
  if (m_is_null)
  {
    return 0;
  }
  int8_t to_int;
  Protocol_chunk<int8_t> prot_integer(to_int);

  buffer_source buff(m_storage, m_size);
  buff >> prot_integer;
  return to_int;
}

int16_t Value::as_int16() const
{
  if (m_is_null)
  {
    return 0;
  }
  int16_t to_int;
  Protocol_chunk<int16_t> prot_integer(to_int);

  buffer_source buff(m_storage, m_size);
  buff >> prot_integer;
  return to_int;
}

int32_t Value::as_int24() const
{
  if (m_is_null)
  {
    return 0;
  }
  int32_t to_int;
  char high_byte = (m_storage[2] & 0x80) == 0 ? 0 : -1;

  to_int = static_cast<int32_t >(((high_byte & 0xff) << 24) |
                                ((m_storage[2] & 0xff) << 16) |
                                ((m_storage[1] & 0xff) << 8) |
                                 (m_storage[0] & 0xff));
  return to_int;
}

int64_t Value::as_int64() const
{
  if (m_is_null)
  {
    return 0;
  }
  int64_t to_int;
  Protocol_chunk<int64_t> prot_integer(to_int);

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

int64_t read_int8(char *& data)
{
  int64_t value;
  value = static_cast<int8_t >(data[0]);
  data++;

  return value;
}

int64_t read_int16_be(char *& data)
{
  int64_t value;
  value = static_cast<int16_t >(((data[0] & 0xff) << 8) |
                                       (data[1] & 0xff));
  data += 2;
  return value;
}

int64_t read_int24_be(char *& data)
{
  int64_t value;
  char high_byte = (data[0] & 0x80) == 0 ? 0 : -1;
  value = static_cast<int32_t >(((high_byte & 0xff) << 24) |
                                       ((data[0] & 0xff) << 16) |
                                       ((data[1] & 0xff) << 8) |
                                        (data[2] & 0xff));
  data += 3;
  return value;
}

int64_t read_int32_be(char *& data)
{
  int64_t value;
  value = static_cast<int32_t >(((data[0] & 0xff) << 24) |
                                        ((data[1] & 0xff) << 16) |
                                        ((data[2] & 0xff) << 8) |
                                        (data[3] & 0xff));
  data += 4;

  return value;
}

int64_t read_int_be_by_size(int size, char *& data)
{
  int64_t value;
  char buffer[20];

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
      sprintf(buffer, "%u", size);
      throw std::length_error("size " + (std::string)buffer + " not implemented");
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

  uint32_t metadata = val.metadata();
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
  int64_t value = val_ptr[0];
  int64_t mask;
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
  char buffer[20];
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
      sprintf(buffer, "%i", val.as_int8());
      str= buffer;
      break;
    case MYSQL_TYPE_SHORT:
      sprintf(buffer, "%i", val.as_int16());
      str= buffer;
      break;
    case MYSQL_TYPE_LONG:
      sprintf(buffer, "%i", val.as_int32());
      str= buffer;
      break;
    case MYSQL_TYPE_FLOAT:
      sprintf(buffer, "%g", val.as_float());
      str= buffer;
      break;
    case MYSQL_TYPE_DOUBLE:
      sprintf(buffer, "%g", val.as_double());
      str= buffer;
      break;
    case MYSQL_TYPE_NULL:
      str= "not implemented";
      break;
    case MYSQL_TYPE_TIMESTAMP:
      {
        uint32_t val_uint = (uint32_t)val.as_int32();
        if (val_uint == 0) {
          str= "0000-00-00 00:00:00";
        } else {
          sprintf(buffer, "%i", val_uint);
          str= buffer;
        }
      }
      break;
    case MYSQL_TYPE_LONGLONG:
      sprintf(buffer, "%lld", val.as_int64());
      str= buffer;
      break;
    case MYSQL_TYPE_INT24:
      sprintf(buffer, "%i", val.as_int24());
      str= buffer;
      break;
    case MYSQL_TYPE_DATE:
    {
      const char* val_storage = val.storage();
      unsigned int date_val = (val_storage[0] & 0xff) + ((val_storage[1] & 0xff) << 8) + ((val_storage[2] & 0xff) << 16);
      unsigned int date_year = date_val >> 9;
      date_val -= (date_year << 9);
      unsigned int date_month = date_val >> 5;
      unsigned int date_day = date_val - (date_month << 5);
      //str = boost::str(boost::format("%04d-%02d-%02d") % date_year % date_month % date_day);
      sprintf(buffer, "%04d-%02d-%02d", date_year, date_month, date_day);
      str= buffer;
      break;
    }
    case MYSQL_TYPE_DATETIME:
    {
      uint64_t timestamp= val.as_int64();
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
    case MYSQL_TYPE_TIME:
    {
      const char* val_storage = val.storage();
      unsigned int time_val = (val_storage[0] & 0xff) + ((val_storage[1] & 0xff) << 8) + ((val_storage[2] & 0xff) << 16);
      unsigned int time_sec = time_val % 100;
      time_val -= time_sec;
      unsigned int time_min = (time_val % 10000) / 100;
      unsigned int time_hour = (time_val - time_min) / 10000;
      //str = boost::str(boost::format("%02d:%02d:%02d") % time_hour % time_min % time_sec);
      sprintf(buffer, "%02d:%02d:%02d", time_hour, time_min, time_sec);
      str= buffer;
      break;
    }
    case MYSQL_TYPE_YEAR:
    {
      const char* val_storage = val.storage();
      unsigned int year_val = (val_storage[0] & 0xff);
      year_val = year_val > 0 ? (year_val + 1900) : 0;
      //str = boost::str(boost::format("%04d") % year_val);
      sprintf(buffer, "%04d", year_val);
      str= buffer;
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

      if (str_type == MYSQL_TYPE_SET) {
        const char* val_ptr = val.storage();
        const int val_length = val.length();
        uint64_t set_value = 0;
        // length is 1, 2, 4 or 8
        for ( int i = 0; i < val_length; i++ ) {
          set_value += (static_cast<uint64_t>(val_ptr[i]) & 0xff) << ( 8 * i );
        }
        sprintf(buffer, "%llu", set_value);
        str= buffer;
        break;
      } else if (str_type == MYSQL_TYPE_ENUM) {
        unsigned int val_storage = static_cast<unsigned int>(*val.storage());
        //str = boost::str(boost::format("%u") % val_storage);
        sprintf(buffer, "%u", val_storage);
        str= buffer;
        break;
      }

      unsigned long size;
      char *ptr= val.as_c_str(size);
      str.append(ptr, size);
    }
      break;
    case MYSQL_TYPE_BIT:
    {
      const char* val_ptr = val.storage();
      const int val_length = val.length();
      uint64_t bit_value = 0;

      // length is 1, 2, 4 or 8
      for ( int i = val_length - 1, cnt = 0; i >= 0; i--, cnt++ ) {
        bit_value += (static_cast<uint64_t>(val_ptr[i]) & 0xff) << ( 8 * cnt );
      }
      sprintf(buffer, "%llu", bit_value);
      str= buffer;
    }
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
    case MYSQL_TYPE_TIME2:
    {
      char buf[MAX_DATE_STRING_REP_LENGTH];
      MYSQL_TIME ltime;
      longlong packed= my_time_packed_from_binary((uchar *)val.storage(), val.metadata());
      TIME_from_longlong_time_packed(&ltime, packed);
      int buflen= my_time_to_str(&ltime, buf, val.metadata());
      sprintf(buffer, "%s", buf);
      str= buffer;
    }
      break;
    case MYSQL_TYPE_TIMESTAMP2:
    {
      // snip from mysql/sql/log_event#log_event_print_value
      char buf[MAX_DATE_STRING_REP_LENGTH];
      struct timeval tm;
      my_timestamp_from_binary(&tm, (uchar *)val.storage(), val.metadata());
      int buflen= my_timeval_to_str(&tm, buf, val.metadata());
      sprintf(buffer, "%s", buf);
      str= buffer;
      // Return '0000-00-00 00:00:00' for '0' timestamp value,
      // to make it general format.
      // '0' will be shown as '0000-00-00 00:00:00' on only mysql,
      // but '0' will be '1970-01-01 00:00:00' on other databases.
      float str_f;
      sscanf(str.c_str(), "%f", &str_f);
      if (str_f == 0.0) {
        str= "0000-00-00 00:00:00" + str.substr(1);  // add fraction part
      }
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
      sprintf(buffer, "%s", buf);
      str= buffer;
    }
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
      out=(uint32_t)val.as_int32();
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
      out= atol(str.c_str());
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
