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

#include "binlog_driver.h"

namespace mysql { namespace system {

/*
Binary_log_event* Binary_log_driver::parse_event(boost::asio::streambuf
                                                 &sbuff, Log_event_header
                                                 *header)
                                                 */
Binary_log_event* Binary_log_driver::parse_event(std::istream &is,
                                                 Log_event_header *header)
{
  std::cout << "parse_event(eventid:" << (int)header->type_code
   << ", next_position:" << header->next_position
   << ")\n";
  Binary_log_event *parsed_event= 0;

  boost::uint32_t event_length= header->event_length;

  if (m_checksum_alg == BINLOG_CHECKSUM_ALG_CRC32) {
    event_length -= CHECKSUM_CRC32_SIGNATURE_LEN;
  }

  switch (header->type_code) {
    case TABLE_MAP_EVENT:
      parsed_event= proto_table_map_event(is, header, event_length);
      break;
    case QUERY_EVENT:
      parsed_event= proto_query_event(is, header, event_length);
      break;
    case INCIDENT_EVENT:
      parsed_event= proto_incident_event(is, header, event_length);
      break;
    case WRITE_ROWS_EVENT:
    case UPDATE_ROWS_EVENT:
    case DELETE_ROWS_EVENT:
      parsed_event= proto_rows_event(is, header, event_length);
      break;
    case ROTATE_EVENT:
      {
        Rotate_event *rot= proto_rotate_event(is, header, event_length);
        m_binlog_file_name= rot->binlog_file;
        m_binlog_offset= (unsigned long)rot->binlog_pos;
        parsed_event= rot;
      }
      break;
    case INTVAR_EVENT:
      parsed_event= proto_intvar_event(is, header, event_length);
      break;
    case USER_VAR_EVENT:
      parsed_event= proto_uservar_event(is, header, event_length);
      break;
    case FORMAT_DESCRIPTION_EVENT:
      {
        std::cout << "FD event\n";
        std::string buf;
        // The length of the payload is unknown until we get the common header
        // length which is stored in the stream itself.  So, read the stream to
        // the location where the common header length is stored.
        boost::uint32_t len = ST_COMMON_HEADER_LEN_OFFSET + 1;
        std::cout << "len: " << len << "\n";
        for (int i=0; i< len; i++)
        {
          char ch;
          is.get(ch);
          std::cout << "ch:" << (int)ch << "\n";
          buf.push_back(ch);
          if (i == ST_COMMON_HEADER_LEN_OFFSET) {
            int common_header_len = ch;
            // Now we know the correct payload size.  update the length.
            len = header->event_length - common_header_len;
            std::cout << "len(final): " << len << "\n";
          }
        }
        std::cout << "buf: " << buf << "\n";
        is.seekg(-len, is.cur);
        std::cout << "calling get_checksum_alg\n";
        m_checksum_alg= get_checksum_alg(buf.data(), len);
        std::cout << "m_checksum_alg: " << (int)m_checksum_alg << "\n";
        parsed_event= new Binary_log_event(header);
        std::cout << "FD done\n";
      }
      break;
    default:
      {
        // Create a dummy driver.
        parsed_event= new Binary_log_event(header);
      }
  }

  return parsed_event;
}

}
}
