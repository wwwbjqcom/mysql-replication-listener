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

#include "binlog_event.h"
#include <iostream>
namespace mysql
{

namespace system {

const char *get_event_type_str(Log_event_type type)
{
  switch(type) {
  case START_EVENT_V3:  return "Start_v3";
  case STOP_EVENT:   return "Stop";
  case QUERY_EVENT:  return "Query";
  case ROTATE_EVENT: return "Rotate";
  case INTVAR_EVENT: return "Intvar";
  case LOAD_EVENT:   return "Load";
  case NEW_LOAD_EVENT:   return "New_load";
  case SLAVE_EVENT:  return "Slave";
  case CREATE_FILE_EVENT: return "Create_file";
  case APPEND_BLOCK_EVENT: return "Append_block";
  case DELETE_FILE_EVENT: return "Delete_file";
  case EXEC_LOAD_EVENT: return "Exec_load";
  case RAND_EVENT: return "RAND";
  case XID_EVENT: return "Xid";
  case USER_VAR_EVENT: return "User var";
  case FORMAT_DESCRIPTION_EVENT: return "Format_desc";
  case TABLE_MAP_EVENT: return "Table_map";
  case PRE_GA_WRITE_ROWS_EVENT: return "Write_rows_event_old";
  case PRE_GA_UPDATE_ROWS_EVENT: return "Update_rows_event_old";
  case PRE_GA_DELETE_ROWS_EVENT: return "Delete_rows_event_old";
  case WRITE_ROWS_EVENT: return "Write_rows";
  case UPDATE_ROWS_EVENT: return "Update_rows";
  case DELETE_ROWS_EVENT: return "Delete_rows";
  case BEGIN_LOAD_QUERY_EVENT: return "Begin_load_query";
  case EXECUTE_LOAD_QUERY_EVENT: return "Execute_load_query";
  case INCIDENT_EVENT: return "Incident";
  case USER_DEFINED: return "User defined";
  default: return "Unknown";
  }
}

} // end namespace system


Binary_log_event::~Binary_log_event()
{
}


Binary_log_event * create_incident_event(unsigned int type, const char *message, unsigned long pos)
{
  Incident_event *incident= new Incident_event();
  incident->header()->type_code= INCIDENT_EVENT;
  incident->header()->next_position= pos;
  incident->header()->event_length= LOG_EVENT_HEADER_SIZE + 2 + strlen(message);
  incident->type= type;
  incident->message.append(message);
  return incident;
}

/* 
   replication event checksum is introduced in the following "checksum-home" version.
   The checksum-aware servers extract FD's version to decide whether the FD event
   carries checksum info.
*/
const boost::uint8_t checksum_version_split[3]= {5, 6, 1};
const boost::uint32_t checksum_version_product=
  (checksum_version_split[0] * 256 + checksum_version_split[1]) * 256 +
  checksum_version_split[2];

/**
   @param payload_buf buffer holding serialized FD event (no header)
   @param len netto (possible checksum is stripped off) length of the event buf
   
   @return  the version-safe checksum alg descriptor where zero
            designates no checksum, 255 - the orginator is
            checksum-unaware (effectively no checksum) and the actuall
            [1-254] range alg descriptor.
*/
boost::uint8_t get_checksum_alg(const char* payload_buf, boost::uint32_t len)
{
  boost::uint8_t ret;
  char version[ST_SERVER_VER_LEN];
  boost::uint8_t version_split[3];

  memcpy(version, payload_buf + ST_SERVER_VER_OFFSET, ST_SERVER_VER_LEN);
  version[ST_SERVER_VER_LEN - 1]= 0;
  
  do_server_version_split(version, version_split);
  ret= (version_product(version_split) < checksum_version_product) ?
    (boost::uint8_t) BINLOG_CHECKSUM_ALG_UNDEF :
    * (boost::uint8_t*) (payload_buf + len - BINLOG_CHECKSUM_LEN - BINLOG_CHECKSUM_ALG_DESC_LEN);
  return ret;
}

} // end namespace mysql
