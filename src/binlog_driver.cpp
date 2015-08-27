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
   << ",file:" << m_binlog_file_name
   << ",pos:" << m_binlog_offset
   << ")\n";
  Binary_log_event *parsed_event= 0;

  switch (header->type_code) {
    case TABLE_MAP_EVENT:
      parsed_event= proto_table_map_event(is, header);
      break;
    case QUERY_EVENT:
      parsed_event= proto_query_event(is, header);
      break;
    case INCIDENT_EVENT:
      parsed_event= proto_incident_event(is, header);
      break;
    case WRITE_ROWS_EVENT:
    case UPDATE_ROWS_EVENT:
    case DELETE_ROWS_EVENT:
      parsed_event= proto_rows_event(is, header);
      break;
    case ROTATE_EVENT:
      {
        Rotate_event *rot= proto_rotate_event(is, header);
        m_binlog_file_name= rot->binlog_file;
        m_binlog_offset= (unsigned long)rot->binlog_pos;
        parsed_event= rot;
      }
      break;
    case INTVAR_EVENT:
      parsed_event= proto_intvar_event(is, header);
      break;
    case USER_VAR_EVENT:
      parsed_event= proto_uservar_event(is, header);
      break;
    case FORMAT_DESCRIPTION_EVENT:
      {
        std::cout << "FD event\n";
        std::string buf;
        boost::uint32_t len = header->event_length;
        std::cout << "len: " << len << "\n";
        for (int i=0; i< len; i++)
        {
          char ch;
          is.get(ch);
          buf.push_back(ch);
        }
        std::cout << "buf: " << buf << "\n";
        is.seekg(-len, is.cur);
        std::cout << "calling get_checksum_alg\n";
        m_checksum_alg= get_checksum_alg(buf.data(), len);
        std::cout << "m_checksum_alg: " << m_checksum_alg << "\n";
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
