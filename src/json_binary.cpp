/* Copyright (c) 2015, Oracle and/or its affiliates. All rights reserved.

   This program is free software; you can rediostringstreamstribute it and/or modify
   it under the terms of the GNU General Public License as published by
   the Free Software Foundation; version 2 of the License.

   This program is distributed in the hope that it will be useful,
   but WITHOUT ANY WARRANTY; without even the implied warranty of
   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
   GNU General Public License for more details.

   You should have received a copy of the GNU General Public License
   along with this program; if not, write to the Free Software Foundation,
   51 Franklin Street, Suite 500, Boston, MA 02110-1335 USA */

/*
  (From sql/json_binary.cc v5.7.16)
 */

#include "json_binary.h"
#include "byte_order_generic.h"
#include <algorithm>            // std::min
#include <iostream>

#define JSONB_TYPE_SMALL_OBJECT   0x0
#define JSONB_TYPE_LARGE_OBJECT   0x1
#define JSONB_TYPE_SMALL_ARRAY    0x2
#define JSONB_TYPE_LARGE_ARRAY    0x3
#define JSONB_TYPE_LITERAL        0x4
#define JSONB_TYPE_INT16          0x5
#define JSONB_TYPE_UINT16         0x6
#define JSONB_TYPE_INT32          0x7
#define JSONB_TYPE_UINT32         0x8
#define JSONB_TYPE_INT64          0x9
#define JSONB_TYPE_UINT64         0xA
#define JSONB_TYPE_DOUBLE         0xB
#define JSONB_TYPE_STRING         0xC
#define JSONB_TYPE_OPAQUE         0xF

#define JSONB_NULL_LITERAL        '\x00'
#define JSONB_TRUE_LITERAL        '\x01'
#define JSONB_FALSE_LITERAL       '\x02'

/*
  The size of offset or size fields in the small and the large storage
  format for JSON objects and JSON arrays.
*/
#define SMALL_OFFSET_SIZE         2
#define LARGE_OFFSET_SIZE         4

/*
  The size of key entries for objects when using the small storage
  format or the large storage format. In the small format it is 4
  bytes (2 bytes for key length and 2 bytes for key offset). In the
  large format it is 6 (2 bytes for length, 4 bytes for offset).
*/
#define KEY_ENTRY_SIZE_SMALL      (2 + SMALL_OFFSET_SIZE)
#define KEY_ENTRY_SIZE_LARGE      (2 + LARGE_OFFSET_SIZE)

/*
  The size of value entries for objects or arrays. When using the
  small storage format, the entry size is 3 (1 byte for type, 2 bytes
  for offset). When using the large storage format, it is 5 (1 byte
  for type, 4 bytes for offset).
*/
#define VALUE_ENTRY_SIZE_SMALL    (1 + SMALL_OFFSET_SIZE)
#define VALUE_ENTRY_SIZE_LARGE    (1 + LARGE_OFFSET_SIZE)

#define DBUG_ASSERT(A) assert(A)

namespace json_binary
{

/**
  Read a variable length written by append_variable_length().

  @param[in] data  the buffer to read from
  @param[in] data_length  the maximum number of bytes to read from data
  @param[out] length  the length that was read
  @param[out] num  the number of bytes needed to represent the length
  @return  false on success, true on error
*/
static bool read_variable_length(const char *data, size_t data_length,
                                 size_t *length, size_t *num)
{
  /*
    It takes five bytes to represent UINT_MAX32, which is the largest
    supported length, so don't look any further.
  */
  const size_t max_bytes= std::min(data_length, static_cast<size_t>(5));

  size_t len= 0;
  for (size_t i= 0; i < max_bytes; i++)
  {
    // Get the next 7 bits of the length.
    len|= (data[i] & 0x7f) << (7 * i);
    if ((data[i] & 0x80) == 0)
    {
      // The length shouldn't exceed 32 bits.
      if (len > UINT_MAX32)
        return true;                          /* purecov: inspected */

      // This was the last byte. Return successfully.
      *num= i + 1;
      *length= len;
      return false;
    }
  }

  // No more available bytes. Return true to signal error.
  return true;                                /* purecov: inspected */
}


// Constructor for literals and errors.
Value::Value(enum_type t)
  : m_type(t), m_field_type(), m_data(), m_element_count(), m_length(),
    m_int_value(), m_double_value(), m_large()
{
  DBUG_ASSERT(t == LITERAL_NULL || t == LITERAL_TRUE || t == LITERAL_FALSE ||
              t == ERROR);
}


// Constructor for int and uint.
Value::Value(enum_type t, boost::int64_t val)
  : m_type(t), m_field_type(), m_data(), m_element_count(), m_length(),
    m_int_value(val), m_double_value(), m_large()
{
  DBUG_ASSERT(t == INT || t == UINT);
}


// Constructor for double.
Value::Value(double d)
  : m_type(DOUBLE), m_field_type(), m_data(), m_element_count(), m_length(),
    m_int_value(), m_double_value(d), m_large()
{}


// Constructor for string.
Value::Value(const char *data, size_t len)
  : m_type(STRING), m_field_type(), m_data(data), m_element_count(),
    m_length(len), m_int_value(), m_double_value(), m_large()
{}


// Constructor for arrays and objects.
Value::Value(enum_type t, const char *data, size_t bytes,
             size_t element_count, bool large)
  : m_type(t), m_field_type(), m_data(data), m_element_count(element_count),
    m_length(bytes), m_int_value(), m_double_value(), m_large(large)
{
  DBUG_ASSERT(t == ARRAY || t == OBJECT);
}


// Constructor for opaque values.
Value::Value(mysql::system::enum_field_types ft, const char *data, size_t len)
  : m_type(OPAQUE), m_field_type(ft), m_data(data), m_element_count(),
    m_length(len), m_int_value(), m_double_value(), m_large()
{}


bool Value::is_valid() const
{
  switch (m_type)
  {
  case ERROR:
    return false;
  case ARRAY:
    // Check that all the array elements are valid.
    for (size_t i= 0; i < element_count(); i++)
      if (!element(i).is_valid())
        return false;                         /* purecov: inspected */
    return true;
  case OBJECT:
    {
      /*
        Check that all keys and values are valid, and that the keys come
        in the correct order.
      */
      const char *prev_key= NULL;
      size_t prev_key_len= 0;
      for (size_t i= 0; i < element_count(); i++)
      {
        Value k= key(i);
        if (!k.is_valid() || !element(i).is_valid())
          return false;                       /* purecov: inspected */
        const char *curr_key= k.get_data();
        size_t curr_key_len= k.get_data_length();
        if (i > 0)
        {
          if (prev_key_len > curr_key_len)
            return false;                     /* purecov: inspected */
          if (prev_key_len == curr_key_len &&
              (memcmp(prev_key, curr_key, curr_key_len) >= 0))
            return false;                     /* purecov: inspected */
        }
        prev_key= curr_key;
        prev_key_len= curr_key_len;
      }
      return true;
    }
  default:
    // This is a valid scalar value.
    return true;
  }
}


/**
  Get a pointer to the beginning of the STRING or OPAQUE data
  represented by this instance.
*/
const char *Value::get_data() const
{
  DBUG_ASSERT(m_type == STRING || m_type == OPAQUE);
  return m_data;
}


/**
  Get the length in bytes of the STRING or OPAQUE value represented by
  this instance.
*/
size_t Value::get_data_length() const
{
  DBUG_ASSERT(m_type == STRING || m_type == OPAQUE);
  return m_length;
}


/**
  Get the value of an INT.
*/
boost::int64_t Value::get_int64() const
{
  DBUG_ASSERT(m_type == INT);
  return m_int_value;
}


/**
  Get the value of a UINT.
*/
boost::uint64_t Value::get_uint64() const
{
  DBUG_ASSERT(m_type == UINT);
  return static_cast<boost::uint64_t>(m_int_value);
}


/**
  Get the value of a DOUBLE.
*/
double Value::get_double() const
{
  DBUG_ASSERT(m_type == DOUBLE);
  return m_double_value;
}


/**
  Get the number of elements in an array, or the number of members in
  an object.
*/
size_t Value::element_count() const
{
  DBUG_ASSERT(m_type == ARRAY || m_type == OBJECT);
  return m_element_count;
}


/**
  Get the MySQL field type of an opaque value. Identifies the type of
  the value stored in the data portion of an opaque value.
*/
mysql::system::enum_field_types Value::field_type() const
{
  DBUG_ASSERT(m_type == OPAQUE);
  return m_field_type;
}


/**
  Create a Value object that represents an error condition.
*/
static Value err()
{
  return Value(Value::ERROR);
}


/**
  Parse a JSON scalar value.

  @param type   the binary type of the scalar
  @param data   pointer to the start of the binary representation of the scalar
  @param len    the maximum number of bytes to read from data
  @return  an object that represents the scalar value
*/
static Value parse_scalar(boost::uint8_t type, const char *data, size_t len)
{
  switch (type)
  {
  case JSONB_TYPE_LITERAL:
    if (len < 1)
      return err();                           /* purecov: inspected */
    switch (static_cast<boost::uint8_t>(*data))
    {
    case JSONB_NULL_LITERAL:
      return Value(Value::LITERAL_NULL);
    case JSONB_TRUE_LITERAL:
      return Value(Value::LITERAL_TRUE);
    case JSONB_FALSE_LITERAL:
      return Value(Value::LITERAL_FALSE);
    default:
      return err();                           /* purecov: inspected */
    }
  case JSONB_TYPE_INT16:
    if (len < 2)
      return err();                           /* purecov: inspected */
    return Value(Value::INT, sint2korr(data));
  case JSONB_TYPE_INT32:
    if (len < 4)
      return err();                           /* purecov: inspected */
    return Value(Value::INT, sint4korr(data));
  case JSONB_TYPE_INT64:
    if (len < 8)
      return err();                           /* purecov: inspected */
    return Value(Value::INT, sint8korr(data));
  case JSONB_TYPE_UINT16:
    if (len < 2)
      return err();                           /* purecov: inspected */
    return Value(Value::UINT, uint2korr(data));
  case JSONB_TYPE_UINT32:
    if (len < 4)
      return err();                           /* purecov: inspected */
    return Value(Value::UINT, uint4korr(data));
  case JSONB_TYPE_UINT64:
    if (len < 8)
      return err();                           /* purecov: inspected */
    return Value(Value::UINT, uint8korr(data));
  case JSONB_TYPE_DOUBLE:
    {
      if (len < 8)
        return err();                         /* purecov: inspected */
      double d;
      float8get(&d, data);
      return Value(d);
    }
  case JSONB_TYPE_STRING:
    {
      size_t str_len;
      size_t n;
      if (read_variable_length(data, len, &str_len, &n))
        return err();                         /* purecov: inspected */
      if (len < n + str_len)
        return err();                         /* purecov: inspected */
      return Value(data + n, str_len);
    }
  case JSONB_TYPE_OPAQUE:
    {
      /*
        There should always be at least one byte, which tells the field
        type of the opaque value.
      */
      if (len < 1)
        return err();                         /* purecov: inspected */

      // The type is encoded as a uint8 that maps to an enum_field_types.
      boost::uint8_t type_byte= static_cast<boost::uint8_t>(*data);
      mysql::system::enum_field_types field_type= static_cast<mysql::system::enum_field_types>(type_byte);

      // Then there's the length of the value.
      size_t val_len;
      size_t n;
      if (read_variable_length(data + 1, len - 1, &val_len, &n))
        return err();                         /* purecov: inspected */
      if (len < 1 + n + val_len)
        return err();                         /* purecov: inspected */
      return Value(field_type, data + 1 + n, val_len);
    }
  default:
    // Not a valid scalar type.
    return err();
  }
}


/**
  Read an offset or size field from a buffer. The offset could be either
  a two byte unsigned integer or a four byte unsigned integer.

  @param data  the buffer to read from
  @param large tells if the large or small storage format is used; true
               means read four bytes, false means read two bytes
*/
static size_t read_offset_or_size(const char *data, bool large)
{
  return large ? uint4korr(data) : uint2korr(data);
}


/**
  Parse a JSON array or object.

  @param t      type (either ARRAY or OBJECT)
  @param data   pointer to the start of the array or object
  @param len    the maximum number of bytes to read from data
  @param large  if true, the array or object is stored using the large
                storage format; otherwise, it is stored using the small
                storage format
  @return  an object that allows access to the array or object
*/
static Value parse_array_or_object(Value::enum_type t, const char *data,
                                   size_t len, bool large)
{
  DBUG_ASSERT(t == Value::ARRAY || t == Value::OBJECT);

  /*
    Make sure the document is long enough to contain the two length fields
    (both number of elements or members, and number of bytes).
  */
  const size_t offset_size= large ? LARGE_OFFSET_SIZE : SMALL_OFFSET_SIZE;
  if (len < 2 * offset_size)
    return err();
  const size_t element_count= read_offset_or_size(data, large);
  const size_t bytes= read_offset_or_size(data + offset_size, large);

  // The value can't have more bytes than what's available in the data buffer.
  if (bytes > len)
    return err();

  /*
    Calculate the size of the header. It consists of:
    - two length fields
    - if it is a JSON object, key entries with pointers to where the keys
      are stored
    - value entries with pointers to where the actual values are stored
  */
  size_t header_size= 2 * offset_size;
  if (t == Value::OBJECT)
    header_size+= element_count *
      (large ? KEY_ENTRY_SIZE_LARGE : KEY_ENTRY_SIZE_SMALL);
  header_size+= element_count *
    (large ? VALUE_ENTRY_SIZE_LARGE : VALUE_ENTRY_SIZE_SMALL);

  // The header should not be larger than the full size of the value.
  if (header_size > bytes)
    return err();                             /* purecov: inspected */

  return Value(t, data, bytes, element_count, large);
}


/**
  Parse a JSON value within a larger JSON document.

  @param type   the binary type of the value to parse
  @param data   pointer to the start of the binary representation of the value
  @param len    the maximum number of bytes to read from data
  @return  an object that allows access to the value
*/
static Value parse_value(boost::uint8_t type, const char *data, size_t len)
{
  switch (type)
  {
  case JSONB_TYPE_SMALL_OBJECT:
    return parse_array_or_object(Value::OBJECT, data, len, false);
  case JSONB_TYPE_LARGE_OBJECT:
    return parse_array_or_object(Value::OBJECT, data, len, true);
  case JSONB_TYPE_SMALL_ARRAY:
    return parse_array_or_object(Value::ARRAY, data, len, false);
  case JSONB_TYPE_LARGE_ARRAY:
    return parse_array_or_object(Value::ARRAY, data, len, true);
  default:
    return parse_scalar(type, data, len);
  }
}


Value parse_binary(const char *data, size_t len)
{
  // Each document should start with a one-byte type specifier.
  if (len < 1)
    return err();                             /* purecov: inspected */

  return parse_value(data[0], data + 1, len - 1);
}


/**
  Get the element at the specified position of a JSON array or a JSON
  object. When called on a JSON object, it returns the value
  associated with the key returned by key(pos).

  @param pos  the index of the element
  @return a value representing the specified element, or a value where
  type() returns ERROR if pos does not point to an element
*/
Value Value::element(size_t pos) const
{
  DBUG_ASSERT(m_type == ARRAY || m_type == OBJECT);

  if (pos >= m_element_count)
    return err();

  /*
    Value entries come after the two length fields if it's an array, or
    after the two length fields and all the key entries if it's an object.
  */
  size_t first_entry_offset=
    2 * (m_large ? LARGE_OFFSET_SIZE : SMALL_OFFSET_SIZE);
  if (type() == OBJECT)
    first_entry_offset+=
      m_element_count * (m_large ? KEY_ENTRY_SIZE_LARGE : KEY_ENTRY_SIZE_SMALL);

  const size_t entry_size=
    m_large ? VALUE_ENTRY_SIZE_LARGE : VALUE_ENTRY_SIZE_SMALL;
  const size_t entry_offset= first_entry_offset + entry_size * pos;

  boost::uint8_t type= m_data[entry_offset];

  /*
    Check if this is an inlined scalar value. If so, return it.
    The scalar will be inlined just after the byte that identifies the
    type, so it's found on entry_offset + 1.
  */
  if (type == JSONB_TYPE_INT16 || type == JSONB_TYPE_UINT16 ||
      type == JSONB_TYPE_LITERAL ||
      (m_large && (type == JSONB_TYPE_INT32 || type == JSONB_TYPE_UINT32)))
    return parse_scalar(type, m_data + entry_offset + 1, entry_size - 1);

  /*
    Otherwise, it's a non-inlined value, and the offset to where the value
    is stored, can be found right after the type byte in the entry.
  */
  size_t value_offset= read_offset_or_size(m_data + entry_offset + 1, m_large);

  if (m_length < value_offset)
    return err();                             /* purecov: inspected */

  return parse_value(type, m_data + value_offset, m_length - value_offset);
}


/**
  Get the key of the member stored at the specified position in a JSON
  object.

  @param pos  the index of the member
  @return the key of the specified member, or a value where type()
  returns ERROR if pos does not point to a member
*/
Value Value::key(size_t pos) const
{
  DBUG_ASSERT(m_type == OBJECT);

  if (pos >= m_element_count)
    return err();

  const size_t offset_size= m_large ? LARGE_OFFSET_SIZE : SMALL_OFFSET_SIZE;
  const size_t key_entry_size=
    m_large ? KEY_ENTRY_SIZE_LARGE : KEY_ENTRY_SIZE_SMALL;
  const size_t value_entry_size=
    m_large ? VALUE_ENTRY_SIZE_LARGE : VALUE_ENTRY_SIZE_SMALL;

  // The key entries are located after two length fields of size offset_size.
  const size_t entry_offset= 2 * offset_size + key_entry_size * pos;

  // The offset of the key is the first part of the key entry.
  const size_t key_offset= read_offset_or_size(m_data + entry_offset, m_large);

  // The length of the key is the second part of the entry, always two bytes.
  const size_t key_length= uint2korr(m_data + entry_offset + offset_size);

  /*
    The key must start somewhere after the last value entry, and it must
    end before the end of the m_data buffer.
  */
  if ((key_offset < entry_offset +
                    (m_element_count - pos) * key_entry_size +
                    m_element_count * value_entry_size) ||
      (m_length < key_offset + key_length))
    return err();                             /* purecov: inspected */

  return Value(m_data + key_offset, key_length);
}


/**
  Get the value associated with the specified key in a JSON object.

  @param[in] key  pointer to the key
  @param[in] len  length of the key
  @return the value associated with the key, if there is one. otherwise,
  returns ERROR
*/
Value Value::lookup(const char *key, size_t len) const
{
  DBUG_ASSERT(m_type == OBJECT);

  const size_t offset_size=
    (m_large ? LARGE_OFFSET_SIZE : SMALL_OFFSET_SIZE);

  const size_t entry_size=
    (m_large ? KEY_ENTRY_SIZE_LARGE : KEY_ENTRY_SIZE_SMALL);

  // The first key entry is located right after the two length fields.
  const size_t first_entry_offset= 2 * offset_size;

  size_t lo= 0U;                // lower bound for binary search (inclusive)
  size_t hi= m_element_count;   // upper bound for binary search (exclusive)

  while (lo < hi)
  {
    // Find the entry in the middle of the search interval.
    size_t idx= (lo + hi) / 2;
    size_t entry_offset= first_entry_offset + idx * entry_size;

    // Keys are ordered on length, so check length first.
    size_t key_len= uint2korr(m_data + entry_offset + offset_size);
    if (len > key_len)
      lo= idx + 1;
    else if (len < key_len)
      hi= idx;
    else
    {
      // The keys had the same length, so compare their contents.
      size_t key_offset= read_offset_or_size(m_data + entry_offset, m_large);

      int cmp= memcmp(key, m_data + key_offset, len);
      if (cmp > 0)
        lo= idx + 1;
      else if (cmp < 0)
        hi= idx;
      else
        return element(idx);
    }
  }

  return err();
}


/**
  Reserve space in a buffer. In order to avoid frequent reallocations,
  allocate a new buffer at least as twice as large as the current
  buffer if there is not enough space.

  @param[in, out] buffer  the buffer in which to reserve space
  @param[in]      needed  the number of bytes to reserve
  @return false if successful, true if memory could not be allocated
*/
static void reserve(std::string &buf, size_t needed)
{
  size_t size =buf.size();
  size += needed;
  buf.reserve(size);
}

/*
  _dig_vec arrays

  (From strings/int2str.c v5.7.16)
*/
char _dig_vec_upper[] =
  "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZ";
char _dig_vec_lower[] =
  "0123456789abcdefghijklmnopqrstuvwxyz";

/**
  Perform quoting on a JSON string to make an external representation
  of it. it wraps double quotes (text quotes) around the string (cptr)
  an also performs escaping according to the following table:
  <pre>
  Common name     C-style  Original unescaped     Transformed to
                  escape   UTF-8 bytes            escape sequence
                  notation                        in UTF-8 bytes
  ---------------------------------------------------------------
  quote           \"       %x22                    %x5C %x22
  backslash       \\       %x5C                    %x5C %x5C
  backspace       \b       %x08                    %x5C %x62
  formfeed        \f       %x0C                    %x5C %x66
  linefeed        \n       %x0A                    %x5C %x6E
  carriage-return \r       %x0D                    %x5C %x72
  tab             \t       %x09                    %x5C %x74
  unicode         \uXXXX  A hex number in the      %x5C %x75
                          range of 00-1F,          followed by
                          except for the ones      4 hex digits
                          handled above (backspace,
                          formfeed, linefeed,
                          carriage-return,
                          and tab).
  ---------------------------------------------------------------
  </pre>

  @param[in] cptr pointer to string data
  @param[in] length the length of the string
  @param[in,out] buf the destination buffer
  @retval false on success
  @retval true on error

  (From sql/json_dom.c v5.7.16)
*/
bool double_quote(const char *cptr, size_t length, std::string &buf)
{
  if (length < 1) {
    return true;
  }
  for (size_t i= 0; i < length; i++)
  {
    char esc[2]= {'\\', cptr[i]};
    bool done= true;
    switch (cptr[i])
    {
    case '"' :
    case '\\' :
      break;
    case '\b':
      esc[1]= 'b';
      break;
    case '\f':
      esc[1]= 'f';
      break;
    case '\n':
      esc[1]= 'n';
      break;
    case '\r':
      esc[1]= 'r';
      break;
    case '\t':
      esc[1]= 't';
      break;
    default:
      done= false;
    }

    if (done)
    {
        buf.append(esc, 2);
    }
    else if (((cptr[i] & ~0x7f) == 0) && // bit 8 not set
             (cptr[i] < 0x1f))
    {
      /*
        Unprintable control character, use hex a hexadecimal number.
        The meaning of such a number determined by ISO/IEC 10646.
      */
      reserve(buf, 5);
      buf.append("\\u00");
      buf.append(&_dig_vec_lower[(cptr[i] & 0xf0) >> 4], 1);
      buf.append(&_dig_vec_lower[(cptr[i] & 0x0f)], 1);
    }
    else
    {
      buf.append(&cptr[i], 1);
    }
  }
  return true;
}


bool Value::to_string(std::string &buf)
{
  switch (m_type)
  {
  case ARRAY:
  {
    // Check that all the array elements are valid.
    buf.append("[");
    for (size_t i= 0; i < element_count(); i++) {
      if (i>0) {
        buf.append(", ");
      }
      Value v = element(i);
      v.to_string(buf);
    }
    buf.append("]");
    break;
  }
  case OBJECT:
  {
    buf.append("{");
    for (size_t i= 0; i < element_count(); i++)
    {
      if (i>0) {
        buf.append(", ");
      }
      Value k = key(i);
      Value v = element(i);
      const char *data = k.get_data();
      size_t length = k.get_data_length();

      // Append key
      buf.append("\"");
      double_quote(data, length, buf);
      buf.append("\": ");
      // Append value
      v.to_string(buf);
    }
    buf.append("}");
    break;
  }
  case STRING:
  {
    const char *data= get_data();
    size_t length= get_data_length();
    buf.append("\"");
    double_quote(data, length, buf);
    buf.append("\"");
    break;
  }
  case INT:
  {
    std::ostringstream oss;
    boost::int64_t i = get_int64();
    oss << i;
    buf.append(oss.str());
    break;
  }
  case UINT:
  {
    std::ostringstream oss;
    boost::uint64_t i = get_uint64();
    oss << i;
    buf.append(oss.str());
    break;
  }
  case DOUBLE:
  {
    /*
       TODO: Import dtoa.cc and calls the "my_gcvt()" function for converting a double value to string

       Currently this implemention doesn't handle a double value accurately, if the precision of
       the double value is the maximum precision(= 17 mostly). Since MySQL has its own implementation
       to print a double value, eventually double values need to be converted through the "my_gcvt()"
       function after importing the code in dtoa.cc.

       Examples:
         {"a": 1.8446744073709552e19} => {"a": 1.844674407370955e+19}
         {"a": 1.0000000000000004}    => {"a": 1}

       dtoa.cc
         https://dev.mysql.com/doc/dev/mysql-server/latest/dtoa_8cc.html
    */
    std::ostringstream oss;
    double d = get_double();

    /*
       Since "digits10 + 2" causes another issue for floating-point inaccuracy,
       we decided not to show the last digit of the floating point.

       Examples of issues if 'digits10 + 2' is set:
         {"a": 1.33}                  => {"a": 1.3300000000000001}
         {"a": 1.23456789}            => {"a": 1.2345678899999999}
    */
    oss.precision(std::numeric_limits<double>::digits10 + 1);
    oss << d;
    buf.append(oss.str());
    break;
  }
  case LITERAL_NULL:
  {
    buf.append("null");
    break;
  }
  case LITERAL_TRUE:
  {
    buf.append("true");
    break;
  }
  case LITERAL_FALSE:
  {
    buf.append("false");
    break;
  }
  case OPAQUE:
  {
    // Dump a raw string
    buf.append("\"");
    buf.append(get_data(), get_data_length());
    buf.append("\"");
    break;
  }
  case ERROR:
    buf.append("\"<<<< ERROR type detected >>>>\"");
    return false;
  default:
    buf.append("\"<<<< Unsupported type detected >>>>\"");
    return false;
  }
  return true;
}


} // end namespace json_binary
