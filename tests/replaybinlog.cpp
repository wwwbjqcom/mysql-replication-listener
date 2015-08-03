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
#include <stdlib.h>
#include <boost/foreach.hpp>
#include "binlog_api.h"

bool is_text_field(Value &val)
{
  if (val.type() == mysql::system::MYSQL_TYPE_VARCHAR ||
      val.type() == mysql::system::MYSQL_TYPE_BLOB ||
      val.type() == mysql::system::MYSQL_TYPE_MEDIUM_BLOB ||
      val.type() == mysql::system::MYSQL_TYPE_LONG_BLOB)
    return true;
  return false;
}

void table_insert(const std::string& table_name, mysql::Row_of_fields &fields)
{
  std::cout << "INSERT INTO "
           << table_name
           << " VALUES (";
  mysql::Row_of_fields::iterator field_it= fields.begin();
  mysql::Converter converter;
  do {
    /*
     Each row contains a vector of Value objects. The converter
     allows us to transform the value into another
     representation.
    */
    std::string str;
    converter.to(str, *field_it);
    if (is_text_field(*field_it))
      std::cout << '\'';
    std::cout << str;
    if (is_text_field(*field_it))
      std::cout << '\'';
    ++field_it;
    if (field_it != fields.end())
      std::cout << ", ";
  } while(field_it != fields.end());
  std::cout << ")" << std::endl;
}


void table_update(const std::string& table_name,
                  mysql::Row_of_fields &old_fields, mysql::Row_of_fields &new_fields)
{
  std::cout << "UPDATE "
           << table_name
           << " SET ";

  int field_id= 0;
  mysql::Row_of_fields::iterator field_it= new_fields.begin();
  mysql::Converter converter;
  do {
    std::string str;
    converter.to(str, *field_it);
    std::cout << field_id << "= ";
    if (is_text_field(*field_it))
      std::cout << '\'';
    std::cout << str;
    if (is_text_field(*field_it))
      std::cout << '\'';
    ++field_it;
    ++field_id;
    if (field_it != new_fields.end())
      std::cout << ", ";
  } while(field_it != new_fields.end());
  std::cout << " WHERE ";
  field_it= old_fields.begin();
  field_id= 0;
  do {
    std::string str;
    converter.to(str, *field_it);
    std::cout << field_id << "= ";
    if (is_text_field(*field_it))
      std::cout << '\'';
    std::cout << str;
    if (is_text_field(*field_it))
      std::cout << '\'';
    ++field_it;
    ++field_id;
    if (field_it != old_fields.end())
      std::cout << " AND ";
  } while(field_it != old_fields.end());
  std::cout << " LIMIT 1" << std::endl;

}


void table_delete(const std::string& table_name, mysql::Row_of_fields &fields)
{
  std::cout << "DELETE FROM "
           << table_name
           << " WHERE ";
  mysql::Row_of_fields::iterator field_it= fields.begin();
  int field_id= 0;
  mysql::Converter converter;
  do {

    std::string str;
    converter.to(str, *field_it);
    std::cout << field_id << "= ";
    if (is_text_field(*field_it))
      std::cout << '\'';
    std::cout << str;
    if (is_text_field(*field_it))
      std::cout << '\'';
    ++field_it;
    ++field_id;
    if (field_it != fields.end())
      std::cout << " AND ";
  } while(field_it != fields.end());
  std::cout << " LIMIT 1" << std::endl;
}

class Incident_handler : public mysql::Content_handler
{
public:
 Incident_handler() : mysql::Content_handler() {}

 mysql::Binary_log_event *process_event(mysql::Incident_event *incident)
 {
   std::cout << "Event type: "
             << mysql::system::get_event_type_str(incident->get_event_type())
             << " length: " << incident->header()->event_length
             << " next pos: " << incident->header()->next_position
             << std::endl;
   std::cout << "type= "
             << (unsigned)incident->type
             << " message= "
             << incident->message
             <<  std::endl
             <<  std::endl;
   /* Consume the event */
   delete incident;
   return 0;
 }
};

class Replay_binlog : public mysql::Content_handler
{
public:
  Replay_binlog() : mysql::Content_handler() {}
  mysql::Binary_log_event *process_event(mysql::Binary_log_event *event)
  {
    if (event->get_event_type() != mysql::USER_DEFINED)
      return event;
    std::cout << "Event type: "
            << mysql::system::get_event_type_str(event->get_event_type())
            << " length: " << event->header()->event_length
            << " next pos: " << event->header()->next_position
            << std::endl;
    mysql::Transaction_log_event *trans= static_cast<mysql::Transaction_log_event *>(event);
    int row_count=0;
    /*
      The transaction event we created has aggregated all row events in an
      ordered list.
    */
    BOOST_FOREACH(mysql::Binary_log_event * event, trans->m_events)
    {
      switch (event->get_event_type())
      {
        case mysql::WRITE_ROWS_EVENT:
        case mysql::DELETE_ROWS_EVENT:
        case mysql::UPDATE_ROWS_EVENT:
        mysql::Row_event *rev= static_cast<mysql::Row_event *>(event);
        boost::uint64_t table_id= rev->table_id;
        // BUG: will create a new event header if the table id doesn't exist.
        Binary_log_event * tmevent= trans->table_map()[table_id];
        mysql::Table_map_event *tm;
        if (tmevent != NULL)
          tm= static_cast<mysql::Table_map_event *>(tmevent);
        else
        {
          std::cout << "Table id "
                    << table_id
                    << " was not registered by any preceding table map event."
                    << std::endl;
          continue;
        }
        /*
         Each row event contains multiple rows and fields. The Row_iterator
         allows us to iterate one row at a time.
        */
        mysql::Row_event_set rows(rev, tm);
        /*
         Create a fuly qualified table name
        */
        std::ostringstream os;
        os << tm->db_name << '.' << tm->table_name;
        mysql::Row_event_set::iterator it= rows.begin();
        do {
          mysql::Row_of_fields fields= *it;

          if (event->get_event_type() == mysql::WRITE_ROWS_EVENT)
                 table_insert(os.str(),fields);
          if (event->get_event_type() == mysql::UPDATE_ROWS_EVENT)
          {
            ++it;
            mysql::Row_of_fields fields2= *it;
            table_update(os.str(),fields,fields2);
          }
          if (event->get_event_type() == mysql::DELETE_ROWS_EVENT)
            table_delete(os.str(),fields);
        } while (++it != rows.end());
      } // end switch
    } // end BOOST_FOREACH
    /* Consume the event */
    delete trans;
    return 0;
  }
};
/*
 *
 */
int main(int argc, char** argv)
{
  if (argc < 2)
  {
    fprintf(stderr,"Usage:\n\treplaybinlog URL\n\nExample:\n\treplaybinlog mysql://root:mypasswd@127.0.0.1:3306\n\n");
    return (EXIT_FAILURE);
  }

  mysql::Binary_log binlog(mysql::system::create_transport(argv[1]));

  if (argc > 2)
  {
    // Set ssl ca certificate
    binlog.set_ssl_ca(std::string(argv[2]));

    // Set ssl cipher list
    //binlog.set_ssl_cipher("AES256-SHA");
    //RDS ssl_cipher list
    //binlog.set_ssl_cipher("EXP1024-RC4-SHA:EXP1024-DES-CBC-SHA:AES256-SHA:AES128-SHA:DES-CBC3-SHA:DES-CBC-SHA:EXP-DES-CBC-SHA:EXP-RC2-CBC-MD5:RC4-SHA:RC4-MD5:EXP-RC4-MD5:NULL-SHA:NULL-MD5:DES-CBC3-MD5:DES-CBC-MD5:EXP-RC2-CBC-MD5:RC2-CBC-MD5:EXP-RC4-MD5:RC4-MD5:KRB5-DES-CBC3-MD5:KRB5-DES-CBC3-SHA:ADH-DES-CBC3-SHA:EDH-RSA-DES-CBC3-SHA:EDH-DSS-DES-CBC3-SHA:ADH-AES256-SHA:DHE-RSA-AES256-SHA:DHE-DSS-AES256-SHA:ADH-AES128-SHA:DHE-RSA-AES128-SHA:DHE-DSS-AES128-SHA:EXP-KRB5-RC4-MD5:EXP-KRB5-RC2-CBC-MD5:EXP-KRB5-DES-CBC-MD5:KRB5-RC4-MD5:KRB5-DES-CBC-MD5:ADH-RC4-MD5:EXP-ADH-RC4-MD5:DHE-DSS-RC4-SHA:EXP1024-DHE-DSS-RC4-SHA:EXP1024-DHE-DSS-DES-CBC-SHA:EXP-KRB5-RC4-SHA:EXP-KRB5-RC2-CBC-SHA:EXP-KRB5-DES-CBC-SHA:KRB5-RC4-SHA:KRB5-DES-CBC-SHA:ADH-DES-CBC-SHA:EXP-ADH-DES-CBC-SHA:EDH-RSA-DES-CBC-SHA:EXP-EDH-RSA-DES-CBC-SHA:EDH-DSS-DES-CBC-SHA:EXP-EDH-DSS-DES-CBC-SHA");
  }

  /*
    Attach a custom event parser which produces user defined events
  */
  mysql::Basic_transaction_parser transaction_parser;
  Incident_handler incident_hndlr;
  Replay_binlog replay_hndlr;

  binlog.content_handler_pipeline()->push_back(&transaction_parser);
  binlog.content_handler_pipeline()->push_back(&incident_hndlr);
  binlog.content_handler_pipeline()->push_back(&replay_hndlr);

  if (binlog.connect())
  {
    fprintf(stderr,"Can't connect to the master.\n");
    return (EXIT_FAILURE);
  }

  if (binlog.set_position(4) != ERR_OK)
  {
    fprintf(stderr,"Can't reposition the binary log reader.\n");
    return (EXIT_FAILURE);
  }

  Binary_log_event  *event;

  bool quit= false;
  while(!quit)
  {
    /*
     Pull events from the master. This is the heart beat of the event listener.
    */
    if (binlog.wait_for_next_event(&event))
    {
      quit= true;
      continue;
    }

    /*
     Print the event
    */
    std::cout << "Event type: "
              << mysql::system::get_event_type_str(event->get_event_type())
              << " length: " << event->header()->event_length
              << " next pos: " << event->header()->next_position
              << std::endl;

    /*
     Perform a special action based on event type
    */

    switch(event->header()->type_code)
    {
    case mysql::QUERY_EVENT:
      {
        const mysql::Query_event *qev= static_cast<const mysql::Query_event *>(event);
        std::cout << "query= "
                  << qev->query
                  << " db= "
                  << qev->db_name
                  <<  std::endl
                  <<  std::endl;
        if (qev->query.find("DROP TABLE REPLICATION_LISTENER") != std::string::npos ||
            qev->query.find("DROP TABLE `REPLICATION_LISTENER`") != std::string::npos)
        {
          quit= true;
        }
      }
      break;

    case mysql::ROTATE_EVENT:
      {
        mysql::Rotate_event *rot= static_cast<mysql::Rotate_event *>(event);
        std::cout << "filename= "
                  << rot->binlog_file.c_str()
                  << " pos= "
                  << rot->binlog_pos
                  << std::endl
                  << std::endl;
      }
      break;

    } // end switch
    delete event;
  } // end loop
  return (EXIT_SUCCESS);
}
