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
#ifndef _BINLOG_EVENT_H
#define	_BINLOG_EVENT_H

#include <boost/cstdint.hpp>
#include <list>
#include <boost/asio.hpp>
#include <boost/function.hpp>
#include <vector>

namespace mysql
{
/*
 We could have used SERVER_VERSION_LENGTH, but this introduces an
 obscure dependency - if somebody decided to change SERVER_VERSION_LENGTH
 this would break the replication protocol
*/
#define ST_SERVER_VER_LEN 50

/*
   Fixed header length, where 4.x and 5.0 agree. That is, 5.0 may have a longer
   header (it will for sure when we have the unique event's ID), but at least
   the first 19 bytes are the same in 4.x and 5.0. So when we have the unique
   event's ID, LOG_EVENT_HEADER_LEN will be something like 26, but
   LOG_EVENT_MINIMAL_HEADER_LEN will remain 19.
*/
#define LOG_EVENT_MINIMAL_HEADER_LEN 19U

/* start event post-header (for v3 and v4) */

#define ST_BINLOG_VER_OFFSET  0
#define ST_SERVER_VER_OFFSET  2
#define ST_CREATED_OFFSET     (ST_SERVER_VER_OFFSET + ST_SERVER_VER_LEN)
#define ST_COMMON_HEADER_LEN_OFFSET (ST_CREATED_OFFSET + 4)

enum enum_binlog_checksum_alg {
  BINLOG_CHECKSUM_ALG_OFF= 0,    // Events are without checksum though its generator
                                 // is checksum-capable New Master (NM).
  BINLOG_CHECKSUM_ALG_CRC32= 1,  // CRC32 of zlib algorithm.
  BINLOG_CHECKSUM_ALG_ENUM_END,  // the cut line: valid alg range is [1, 0x7f].
  BINLOG_CHECKSUM_ALG_UNDEF= 255 // special value to tag undetermined yet checksum
                                 // or events from checksum-unaware servers
};

#define CHECKSUM_CRC32_SIGNATURE_LEN 4
/**
   defined statically while there is just one alg implemented
*/
#define BINLOG_CHECKSUM_LEN CHECKSUM_CRC32_SIGNATURE_LEN
#define BINLOG_CHECKSUM_ALG_DESC_LEN 1  /* 1 byte checksum alg descriptor */

/**
  @enum Log_event_type

  Enumeration type for the different types of log events.
*/
enum Log_event_type
{
  /*
    Every time you update this enum (when you add a type), you have to
    fix Format_description_log_event::Format_description_log_event().
  */
  UNKNOWN_EVENT= 0,
  START_EVENT_V3= 1,
  QUERY_EVENT= 2,
  STOP_EVENT= 3,
  ROTATE_EVENT= 4,
  INTVAR_EVENT= 5,
  LOAD_EVENT= 6,
  SLAVE_EVENT= 7,
  CREATE_FILE_EVENT= 8,
  APPEND_BLOCK_EVENT= 9,
  EXEC_LOAD_EVENT= 10,
  DELETE_FILE_EVENT= 11,
  /*
    NEW_LOAD_EVENT is like LOAD_EVENT except that it has a longer
    sql_ex, allowing multibyte TERMINATED BY etc; both types share the
    same class (Load_log_event)
  */
  NEW_LOAD_EVENT= 12,
  RAND_EVENT= 13,
  USER_VAR_EVENT= 14,
  FORMAT_DESCRIPTION_EVENT= 15,
  XID_EVENT= 16,
  BEGIN_LOAD_QUERY_EVENT= 17,
  EXECUTE_LOAD_QUERY_EVENT= 18,

  TABLE_MAP_EVENT = 19,

  /*
    These event numbers were used for 5.1.0 to 5.1.15 and are
    therefore obsolete.
   */
  PRE_GA_WRITE_ROWS_EVENT = 20,
  PRE_GA_UPDATE_ROWS_EVENT = 21,
  PRE_GA_DELETE_ROWS_EVENT = 22,

  /*
    These event numbers are used from 5.1.16 and forward
   */
  WRITE_ROWS_EVENT = 23,
  UPDATE_ROWS_EVENT = 24,
  DELETE_ROWS_EVENT = 25,

  /*
    Something out of the ordinary happened on the master
   */
  INCIDENT_EVENT= 26,

          /*
           * A user defined event
           */
          USER_DEFINED= 27,
  /*
    Add new events here - right above this comment!
    Existing events (except ENUM_END_EVENT) should never change their numbers
  */


  ENUM_END_EVENT /* end marker */
};

namespace system {
/**
 * Convenience function to get the string representation of a binlog event.
 */
const char* get_event_type_str(Log_event_type type);
} // end namespace system

#define LOG_EVENT_HEADER_SIZE 20
class Log_event_header
{
public:
  boost::uint8_t  marker; // always 0 or 0xFF
  boost::uint32_t timestamp;
  boost::uint8_t  type_code;
  boost::uint32_t server_id;
  boost::uint32_t event_length;
  boost::uint32_t next_position;
  boost::uint16_t flags;
};


class Binary_log_event;

/**
 * TODO Base class for events. Implementation is in body()
 */
class Binary_log_event
{
public:
    Binary_log_event()
    {
        /*
          An event length of 0 indicates that the header isn't initialized
         */
        m_header.event_length= 0;
        m_header.type_code=    0;
    }

    Binary_log_event(Log_event_header *header)
    {
        m_header= *header;
    }

    virtual ~Binary_log_event();

    /**
     * Helper method
     */
    enum Log_event_type get_event_type() const
    {
      return (enum Log_event_type) m_header.type_code;
    }

    /**
     * Return a pointer to the header of the log event
     */
    Log_event_header *header() { return &m_header; }

private:
    Log_event_header m_header;
};

class Query_event: public Binary_log_event
{
public:
    Query_event(Log_event_header *header) : Binary_log_event(header) {}
    boost::uint32_t thread_id;
    boost::uint32_t exec_time;
    boost::uint16_t error_code;
    std::vector<boost::uint8_t > variables;

    std::string db_name;
    std::string query;
};

class Rotate_event: public Binary_log_event
{
public:
    Rotate_event(Log_event_header *header) : Binary_log_event(header) {}
    std::string binlog_file;
    boost::uint64_t binlog_pos;
};

class Format_event: public Binary_log_event
{
public:
    Format_event(Log_event_header *header) : Binary_log_event(header) {}
    boost::uint16_t binlog_version;
    std::string master_version;
    boost::uint32_t created_ts;
    boost::uint8_t log_header_len;
};

class User_var_event: public Binary_log_event
{
public:
    enum Value_type {
      STRING_TYPE,
      REAL_TYPE,
      INT_TYPE,
      ROW_TYPE,
      DECIMAL_TYPE,
      VALUE_TYPE_COUNT
    };

    User_var_event(Log_event_header *header) : Binary_log_event(header) {}
    std::string name;
    boost::uint8_t is_null;
    boost::uint8_t type;
    boost::uint32_t charset; /* charset of the string */
    std::string value; /* encoded in binary speak, depends on .type */
};

class Table_map_event: public Binary_log_event
{
public:
    Table_map_event(Log_event_header *header) : Binary_log_event(header) {}
    boost::uint64_t table_id;
    boost::uint16_t flags;
    std::string db_name;
    std::string table_name;
    std::vector<uint8_t> columns;
    std::vector<uint8_t> metadata;
    std::vector<uint8_t> null_bits;
};

class Row_event: public Binary_log_event
{
public:
    Row_event(Log_event_header *header) : Binary_log_event(header) {}
    boost::uint64_t table_id;
    boost::uint16_t flags;
    boost::uint64_t columns_len;
    boost::uint32_t null_bits_len;
    std::vector<boost::uint8_t> columns_before_image;
    std::vector<uint8_t> used_columns;
    std::vector<uint8_t> row;
};

class Int_var_event: public Binary_log_event
{
public:
    Int_var_event(Log_event_header *header) : Binary_log_event(header) {}
    boost::uint8_t  type;
    boost::uint64_t value;
};

class Incident_event: public Binary_log_event
{
public:
    Incident_event() : Binary_log_event() {}
    Incident_event(Log_event_header *header) : Binary_log_event(header) {}
    boost::uint8_t type;
    std::string message;
};

class Xid: public Binary_log_event
{
public:
    Xid(Log_event_header *header) : Binary_log_event(header) {}
    boost::uint64_t xid_id;
};

Binary_log_event *create_incident_event(unsigned int type, const char *message, unsigned long pos= 0);

boost::uint8_t get_checksum_alg(const char* payload_buf, boost::uint32_t len);

inline boost::uint32_t version_product(const boost::uint8_t* version_split)
{
  return ((version_split[0] * 256 + version_split[1]) * 256
          + version_split[2]);
}

/**
   Splits server 'version' string into three numeric pieces stored
   into 'split_versions':
   X.Y.Zabc (X,Y,Z numbers, a not a digit) -> {X,Y,Z}
   X.Yabc -> {X,Y,0}
*/
inline void do_server_version_split(char* version, boost::uint8_t split_versions[3])
{
  char *p= version, *r;
  boost::uint32_t number;
  for (uint i= 0; i<=2; i++)
  {
    number= strtoul(p, &r, 10);
    /*
      It is an invalid version if any version number greater than 255 or
      first number is not followed by '.'.
    */
    if (number < 256 && (*r == '.' || i != 0))
      split_versions[i]= (boost::uint8_t)number;
    else
    {
      split_versions[0]= 0;
      split_versions[1]= 0;
      split_versions[2]= 0;
      break;
    }

    p= r;
    if (*r == '.')
      p++; // skip the dot
  }
}

} // end namespace mysql

#endif	/* _BINLOG_EVENT_H */
