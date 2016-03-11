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
#include "binlog_api.h"
#include <iostream>
#include "tcp_driver.h"


#include <fstream>
#include <time.h>
#include <boost/cstdint.hpp>
#include <streambuf>
#include <stdio.h>
#include <boost/bind.hpp>
#include <boost/thread.hpp>
#include <exception>
#include <boost/foreach.hpp>
#include <openssl/evp.h>
#include <openssl/rand.h>
#include <boost/lexical_cast.hpp>
#include <boost/asio.hpp>
#include <boost/asio/ssl.hpp>
#include <boost/chrono.hpp>

#include "protocol.h"
#include "binlog_event.h"
#include "rowset.h"
#include "field_iterator.h"
#include "binlog_socket.h"

#define GET_NEXT_PACKET_HEADER   \
   m_socket->async_read(boost::asio::buffer(m_net_header, 4), \
     boost::bind(&Binlog_tcp_driver::handle_net_packet_header, this, \
       boost::asio::placeholders::error, boost::asio::placeholders::bytes_transferred)) \

#define PACKET_READ_ERROR 175

using boost::asio::ip::tcp;
using namespace mysql::system;
using namespace mysql;

typedef unsigned char uchar;

namespace mysql { namespace system {

static int encrypt_password(boost::uint8_t *reply,   /* buffer at least EVP_MAX_MD_SIZE */
                            const boost::uint8_t *scramble_buff,
                            const char *pass);
static int hash_sha1(boost::uint8_t *output, ...);

    int Binlog_tcp_driver::connect(const std::string& user, const std::string& passwd,
                                   const std::string& host, long port,
                                   const std::string& binlog_filename, size_t offset)
{
  m_user=user;
  m_passwd=passwd;
  m_host=host;
  m_port=port;

  if (!m_socket)
  {
    if ((m_socket=sync_connect_and_authenticate(m_io_service, user, passwd, host, port)) == 0)
      throw std::runtime_error("Connect or authentication error");
  }

  /**
   * Get the master status if we don't know the name of the file.
   */
  if (binlog_filename == "")
  {
    if (fetch_master_status(m_socket, &m_binlog_file_name, &m_binlog_offset))
      throw std::runtime_error("Error fetching master status");
  } else
  {
    m_binlog_file_name=binlog_filename;
    m_binlog_offset=offset;
  }

  bool checksum_aware_master = false;
  set_master_binlog_checksum(m_socket, checksum_aware_master);
  if (checksum_aware_master) {
    fetch_master_binlog_checksum(m_socket, m_checksum_alg);
  }

  /* We're ready to start the io service and request the binlog dump. */
  start_binlog_dump(m_binlog_file_name, m_binlog_offset);

  return 0;
}



  Binlog_socket* Binlog_tcp_driver::sync_connect_and_authenticate(
    boost::asio::io_service &io_service,
    const std::string &user,
    const std::string &passwd,
    const std::string &host,
    long port)
{

  tcp::resolver resolver(io_service);
  tcp::resolver::query query(host.c_str(), "0");

  boost::system::error_code error=boost::asio::error::host_not_found;

  if (port == 0)
    port= 3306;

  Binlog_socket *binlog_socket;

  if (!m_opt_ssl_ca.empty()) {
    boost::asio::ssl::context *ctx = new boost::asio::ssl::context(io_service, boost::asio::ssl::context::tlsv1);
    ctx->set_verify_mode(boost::asio::ssl::context::verify_peer);

    ctx->load_verify_file(m_opt_ssl_ca);

    binlog_socket = new Binlog_socket(io_service, ctx);

    if (!m_opt_ssl_cipher.empty()) {
      binlog_socket->set_ssl_cipher(m_opt_ssl_cipher);
    }
  } else {
    binlog_socket = new Binlog_socket(io_service);
  }

  tcp::socket* socket = binlog_socket->socket(); // raw socket

  /*
    Try each endpoint until we successfully establish a connection.
   */
  tcp::resolver::iterator endpoint_iterator;
  try {
    endpoint_iterator=resolver.resolve(query);
  } catch (boost::system::system_error e) {
    /*
      Maybe due to a DNS server issue.  Try without DNS lookup, which works if
      the given host is a numeric address.
     */
    boost::system::error_code ec;
    boost::asio::ip::address addr = boost::asio::ip::address::from_string(host, ec);
    if (ec) {
      delete binlog_socket;
      throw std::runtime_error("Host `" + host + "` not found");
    }
    tcp::endpoint ep(addr, port);
    endpoint_iterator=tcp::resolver::iterator::create(ep, host, "0");
  }
  tcp::resolver::iterator end;

  while (error && endpoint_iterator != end)
  {
    /*
      Hack to set port number from a long int instead of a service.
     */
    tcp::endpoint endpoint=endpoint_iterator->endpoint();
    endpoint.port(port);

    socket->close();
    socket->connect(endpoint, error);
    endpoint_iterator++;
  }

  if (error)
  {
    delete binlog_socket;
    throw std::runtime_error("Boost error: " + error.message());
  }

  const char* env_libreplication_tcp_keepalive = std::getenv("LIBREPLICATION_TCP_KEEPALIVE");

  if (env_libreplication_tcp_keepalive != 0) {
    try {
      const int libreplication_tcp_keepalive = boost::lexical_cast<int>(env_libreplication_tcp_keepalive);

      if (libreplication_tcp_keepalive == 1) {
        boost::asio::socket_base::keep_alive tcp_keepalive(true);
        socket->set_option(tcp_keepalive);

#if defined(__linux__) || defined(__MACOSX__)
        const int fd = static_cast<int>(socket->native());

        // TCP_KEEPIDLE
        const char* env_libreplication_tcp_keepidle = std::getenv("LIBREPLICATION_TCP_KEEPIDLE");

        if (env_libreplication_tcp_keepidle != 0) {
          try {
            const int libreplication_tcp_keepidle = boost::lexical_cast<int>(env_libreplication_tcp_keepidle);

            if (libreplication_tcp_keepidle > 0) {
              setsockopt(fd, IPPROTO_TCP, TCP_KEEPIDLE, &libreplication_tcp_keepidle, sizeof(libreplication_tcp_keepidle));
            }
          } catch (boost::bad_lexical_cast e) {
            // XXX: nothing to do
          }
        }

        // TCP_KEEPINTVL
        const char* env_libreplication_tcp_keepintvl = std::getenv("LIBREPLICATION_TCP_KEEPINTVL");

        if (env_libreplication_tcp_keepintvl != 0) {
          try {
            const int libreplication_tcp_keepintvl = boost::lexical_cast<int>(env_libreplication_tcp_keepintvl);

            if (libreplication_tcp_keepintvl > 0) {
              setsockopt(fd, IPPROTO_TCP, TCP_KEEPINTVL, &libreplication_tcp_keepintvl, sizeof(libreplication_tcp_keepintvl));
            }
          } catch (boost::bad_lexical_cast e) {
            // XXX: nothing to do
          }
        }

        // TCP_KEEPCNT
        const char* env_libreplication_tcp_keepcnt = std::getenv("LIBREPLICATION_TCP_KEEPCNT");

        if (env_libreplication_tcp_keepcnt != 0) {
          try {
            const int libreplication_tcp_keepcnt = boost::lexical_cast<int>(env_libreplication_tcp_keepcnt);

            if (libreplication_tcp_keepcnt > 0) {
              setsockopt(fd, IPPROTO_TCP, TCP_KEEPCNT, &libreplication_tcp_keepcnt, sizeof(libreplication_tcp_keepcnt));
            }
          } catch (boost::bad_lexical_cast e) {
            // XXX: nothing to do
          }
        }
#endif

      }
    } catch (boost::bad_lexical_cast e) {
      // XXX: nothing to do
    }
  }

  /*
   * Successfully connected to the master.
   * 1. Accept handshake from server
   * 2. Send authentication package to the server
   * 3. Accept OK server package (or error in case of failure)
   * 4. Send COM_REGISTER_SLAVE command to server
   * 5. Accept OK package from server
   */

  boost::asio::streambuf server_messages;

  /*
   * Get package header
   */
  unsigned long packet_length;
  unsigned char packet_no;
  if (proto_read_package_header(binlog_socket, server_messages, &packet_length, &packet_no))
  {
    delete binlog_socket;
    throw std::runtime_error("Invalid package header");
  }

  /*
   * Get server handshake package
   */
  std::streamsize inbuffer=server_messages.in_avail();
  if (inbuffer < 0)
    inbuffer=0;

  binlog_socket->read(server_messages, boost::asio::transfer_at_least(packet_length - inbuffer));
  std::istream server_stream(&server_messages);
  struct st_handshake_package handshake_package;
  proto_get_handshake_package(server_stream, handshake_package, packet_length);

  /*
   * Set 1 to packet number
   */
  binlog_socket->set_packet_number(1);

  /*
   * SSL start(optional)
   */
  if (binlog_socket->is_ssl())
    start_ssl(binlog_socket, handshake_package);

  /*
   * Authenticate
   */
  if (authenticate(binlog_socket, user, passwd, handshake_package)){
    delete binlog_socket;
    throw std::runtime_error("Authentication failed.");
  }

  /*
   * Register slave to master
   */
  register_slave_to_master(binlog_socket, server_messages, host, port);

  return binlog_socket;
}

    std::size_t write_request(Binlog_socket *binlog_socket,
                              boost::asio::streambuf &request_body_buf,
                              std::size_t packet_number)
{
  int body_size = request_body_buf.size();
  int header_size = 4;
  int total_size = body_size + header_size;

  // Header
  char packet_header[header_size];
  write_packet_header(packet_header, body_size, packet_number);

  /*
   * The following change which sends head and body in one packet
   * triggered a potential bug (segmentation fault error),
   * so decided to comment out.
   * When network latency is very small, like accessing localhost
   * mysql-server, and calling set_position method, the program will be crashed.
  //
  // Entire packet
  boost::asio::streambuf request_buf;
  std::ostream request_stream(&request_buf);
  request_stream.write(packet_header, header_size);
  request_stream << &request_body_buf;

  // Send data to server
  return binlog_socket->write(request_buf, boost::asio::transfer_at_least(total_size));
  */

  binlog_socket->write(boost::asio::buffer(packet_header, 4),
                       boost::asio::transfer_at_least(4));
  return binlog_socket->write(request_body_buf,
                       boost::asio::transfer_at_least(body_size));
}

    std::size_t write_request(Binlog_socket *binlog_socket,
                              boost::asio::streambuf &request_body_buf)
{
  return write_request(binlog_socket, request_body_buf, binlog_socket->get_and_increment_packet_number());
}

    void Binlog_tcp_driver::start_binlog_dump(const std::string &binlog_file_name, size_t offset)
{
  boost::asio::streambuf server_messages;
  std::ostream command_request_stream(&server_messages);

  boost::uint8_t val_command = COM_BINLOG_DUMP;
  Protocol_chunk<boost::uint8_t>  prot_command(val_command);
  boost::uint32_t val_binlog_offset = offset;
  Protocol_chunk<boost::uint32_t> prot_binlog_offset(val_binlog_offset); // binlog position to start at
  boost::uint16_t val_binlog_flags = 0;
  Protocol_chunk<boost::uint16_t> prot_binlog_flags(val_binlog_flags); // not used
  boost::uint32_t val_server_id = 1;
  Protocol_chunk<boost::uint32_t> prot_server_id(val_server_id); // must not be 0; see handshake package

  const char* env_libreplication_server_id = std::getenv("LIBREPLICATION_SERVER_ID");

  if (env_libreplication_server_id != 0) {
    try {
      boost::uint32_t libreplication_server_id = boost::lexical_cast<boost::uint32_t>(env_libreplication_server_id);
      prot_server_id = libreplication_server_id;
    } catch (boost::bad_lexical_cast e) {
      // XXX: nothing to do
    }
  }

  command_request_stream
          << prot_command
          << prot_binlog_offset
          << prot_binlog_flags
          << prot_server_id
          << binlog_file_name;

  // Send request
  write_request(m_socket, server_messages, m_socket->reset_and_increment_packet_number());

  /*
   Start receiving binlog events.
   */
  if (!m_shutdown)
    GET_NEXT_PACKET_HEADER;

  /*
   Start the event loop in a new thread
   */
  if (!m_event_loop)
    m_event_loop= new boost::thread(boost::bind(&Binlog_tcp_driver::start_event_loop, this));

}

/**
 Helper function used to extract the event header from a memory block
 */
static void proto_event_packet_header(boost::asio::streambuf &event_src, Log_event_header *h)
{
  std::istream is(&event_src);

  Protocol_chunk<boost::uint8_t> prot_marker(h->marker);
  Protocol_chunk<boost::uint32_t> prot_timestamp(h->timestamp);
  Protocol_chunk<boost::uint8_t> prot_type_code(h->type_code);
  Protocol_chunk<boost::uint32_t> prot_server_id(h->server_id);
  Protocol_chunk<boost::uint32_t> prot_event_length(h->event_length);
  Protocol_chunk<boost::uint32_t> prot_next_position(h->next_position);
  Protocol_chunk<boost::uint16_t> prot_flags(h->flags);

  is >> prot_marker
          >> prot_timestamp
          >> prot_type_code
          >> prot_server_id
          >> prot_event_length
          >> prot_next_position
          >> prot_flags;
}

void Binlog_tcp_driver::handle_net_packet(const boost::system::error_code& err, std::size_t bytes_transferred)
{
  // std::cerr << "handle_net_packet bytes_transferred:" << bytes_transferred << std::endl;
  if (err)
  {
    // std::cerr << "handle_net_packet was called with error: " << err.message().c_str() << std::endl;
    Binary_log_event * ev= create_incident_event(PACKET_READ_ERROR, err.message().c_str(), m_binlog_offset);
    m_event_queue->push_front(ev);
    return;
  }

  if (bytes_transferred > MAX_PACKAGE_SIZE || bytes_transferred == 0)
  {
    // std::cerr << "bytes_transferred (" << bytes_transferred << ") too big" << std::endl;
    std::ostringstream os;
    os << "Expected byte size to be between 0 and "
       << MAX_PACKAGE_SIZE
       << " number of bytes; got "
       << bytes_transferred
       << " instead.";
    Binary_log_event * ev= create_incident_event(PACKET_READ_ERROR, os.str().c_str(), m_binlog_offset);
    m_event_queue->push_front(ev);
    return;
  }

  //assert(m_waiting_event != 0);
  //std::cerr << "Committing '"<< bytes_transferred << "' bytes. size:" << m_event_stream_buffer.size() << std::endl;
  m_event_stream_buffer.commit(bytes_transferred);
  //std::cerr << "Commit complete. size:" << m_event_stream_buffer.size() << std::endl;
  /*
    If the event object doesn't have an event length it means that the header
    hasn't been parsed. If the event stream also contains enough bytes
    we make the assumption that the next bytes waiting in the stream is
    the event header and attempt to parse it.
   */
  if (m_waiting_event->event_length == 0 && m_event_stream_buffer.size() >= 19)
  {
    /*
      Copy and remove from the event stream, the remaining bytes might be
      dynamic payload.
     */
    //std::cerr << "Consuming event stream for header. Size before: " << m_event_stream_buffer.size() << std::endl;
    proto_event_packet_header(m_event_stream_buffer, m_waiting_event);
    //std::cerr << " Size after: " << m_event_stream_buffer.size() << std::endl;
  }

  //std::cerr << "Event length: " << m_waiting_event->event_length << " and available payload size is " << m_event_stream_buffer.size()+LOG_EVENT_HEADER_SIZE-1 <<  std::endl;
  if (m_waiting_event->event_length == m_event_stream_buffer.size() + LOG_EVENT_HEADER_SIZE - 1)
  {
    /*
     If the header length equals the size of the payload plus the
     size of the header, the event object is complete.
     Next we need to parse the payload buffer
     */
    std::istream is(&m_event_stream_buffer);
    Binary_log_event * event= parse_event(is, m_waiting_event);

    m_event_stream_buffer.consume(m_event_stream_buffer.size());

    m_event_queue->push_front(event);

    /*
      Note on memory management: The pushed Binary_log_event will be
      deleted in user land.
    */
    delete m_waiting_event;
    m_waiting_event= 0;
  }

  if (!m_shutdown)
    GET_NEXT_PACKET_HEADER;

}

void Binlog_tcp_driver::handle_net_packet_header(const boost::system::error_code& err, std::size_t bytes_transferred)
{
  // std::cerr << "handle_net_packet_header bytes_transferred:" << bytes_transferred << std::endl;
  if (err)
  {
    // std::cerr << "handle_net_packet was called with error: "  << err.message().c_str()  << std::endl;
    Binary_log_event * ev= create_incident_event(PACKET_READ_ERROR, err.message().c_str(), m_binlog_offset);
    m_event_queue->push_front(ev);
    return;
  }

  if (bytes_transferred != 4)
  {
    std::ostringstream os;
    os << "Expected byte size to be between 0 and "
       << MAX_PACKAGE_SIZE
       << " number of bytes; got "
       << bytes_transferred
       << " instead.";
    Binary_log_event * ev= create_incident_event(PACKET_READ_ERROR, os.str().c_str(), m_binlog_offset);
    m_event_queue->push_front(ev);
    return;
  }

  int packet_length=(unsigned long) (m_net_header[0] &0xFF);
  packet_length+=(unsigned long) ((m_net_header[1] &0xFF) << 8);
  packet_length+=(unsigned long) ((m_net_header[2] &0xFF) << 16);

  // TODO validate packet sequence numbers
  //int packet_no=(unsigned char) m_net_header[3];

  if (m_waiting_event == 0)
  {
    m_waiting_event= new Log_event_header();
    //assert(m_event_stream_buffer.size() == 0);
  }
  //std::cerr << "event_stream_buffer.prepare:" << packet_length << std::endl;
  m_event_packet=  boost::asio::buffer_cast<char *>(m_event_stream_buffer.prepare(packet_length));

  m_socket->async_read(boost::asio::buffer(m_event_packet, packet_length),
                          boost::bind(&Binlog_tcp_driver::handle_net_packet,
                                      this,
                                      boost::asio::placeholders::error,
                                      boost::asio::placeholders::bytes_transferred));
}

    void start_ssl(Binlog_socket *binlog_socket, struct st_handshake_package &handshake_package)
{

  // SSL Body
  boost::asio::streambuf ssl_request;
  std::ostream ssl_request_stream(&ssl_request);

  boost::uint8_t filler_buffer[23];
  memset((char *) filler_buffer, '\0', 23);

  boost::uint32_t val_client_flags = (boost::uint32_t)CLIENT_SSL_FLAGS;
  Protocol_chunk<boost::uint32_t> prot_client_flags(val_client_flags);
  boost::uint32_t val_max_packet_size = MAX_PACKAGE_SIZE;
  Protocol_chunk<boost::uint32_t> prot_max_packet_size(val_max_packet_size);
  boost::uint8_t val_charset_number = handshake_package.server_language;
  Protocol_chunk<boost::uint8_t>  prot_charset_number(val_charset_number);
  Protocol_chunk<boost::uint8_t>  prot_filler_buffer(filler_buffer, 23);

  ssl_request_stream << prot_client_flags
                     << prot_max_packet_size
                     << prot_charset_number
                     << prot_filler_buffer;

  // Send ssl request
  write_request(binlog_socket, ssl_request, binlog_socket->get_and_increment_packet_number());

  // Handshake for SSL
  binlog_socket->handshake();
}


    int authenticate(Binlog_socket *binlog_socket, const std::string& user,
                     const std::string& passwd,
                     const st_handshake_package &handshake_package)
{
  try
  {
    /*
     * Send authentication package
     */
    // Auth body
    boost::asio::streambuf auth_request;
    std::string database("mysql"); // 0 terminated

    std::ostream auth_request_stream(&auth_request);

    boost::uint8_t filler_buffer[23];
    memset((char *) filler_buffer, '\0', 23);

    boost::uint8_t reply[EVP_MAX_MD_SIZE];
    memset(reply, '\0', EVP_MAX_MD_SIZE);
    boost::uint8_t scramble_buff[21];
    memcpy(scramble_buff, handshake_package.scramble_buff, 8);
    memcpy(scramble_buff+8, handshake_package.scramble_buff2, 13);
    int passwd_length= 0;
    if (passwd.size() > 0)
      passwd_length= encrypt_password(reply, scramble_buff, passwd.c_str());

    boost::uint32_t val_client_flags = (boost::uint32_t)CLIENT_BASIC_FLAGS;
    if (binlog_socket->is_ssl())
      val_client_flags = (boost::uint32_t)CLIENT_SSL_FLAGS;
    Protocol_chunk<boost::uint32_t> prot_client_flags(val_client_flags);
    boost::uint32_t val_max_packet_size = MAX_PACKAGE_SIZE;
    Protocol_chunk<boost::uint32_t> prot_max_packet_size(val_max_packet_size);
    boost::uint8_t val_charset_number = handshake_package.server_language;
    Protocol_chunk<boost::uint8_t>  prot_charset_number(val_charset_number);
    Protocol_chunk<boost::uint8_t>  prot_filler_buffer(filler_buffer, 23);
    boost::uint8_t  val_scramble_buffer_size = (boost::uint8_t) passwd_length;
    Protocol_chunk<boost::uint8_t>  prot_scramble_buffer_size(val_scramble_buffer_size);
    Protocol_chunk<boost::uint8_t>  prot_scamble_buffer((boost::uint8_t *)reply, passwd_length);

    auth_request_stream << prot_client_flags
                        << prot_max_packet_size
                        << prot_charset_number
                        << prot_filler_buffer
                        << user << '\0'
                        << prot_scramble_buffer_size
                        << prot_scamble_buffer
                        << database << '\0';

    // Send auth request
    write_request(binlog_socket, auth_request, binlog_socket->get_and_increment_packet_number());

    /*
     * Get server authentication response
     */
    unsigned long packet_length;
    unsigned char packet_no=1;
    packet_length=proto_get_one_package(binlog_socket, auth_request, &packet_no);

    std::istream auth_response_stream(&auth_request);

    boost::uint8_t result_type;
    Protocol_chunk<boost::uint8_t> prot_result_type(result_type);

    auth_response_stream >> prot_result_type;

    if (result_type == 0)
    {
      struct st_ok_package ok_package;
      prot_parse_ok_message(auth_response_stream, ok_package, packet_length);
    } else
    {
      struct st_error_package error_package;
      prot_parse_error_message(auth_response_stream, error_package, packet_length);
      throw std::runtime_error("Error from server, code=" + boost::lexical_cast<std::string>(error_package.error_code) + ", message=\"" + error_package.message + "\"");
    }

    return 0;
  } catch (boost::system::system_error e)
  {
    throw e;
  }
}

    int register_slave_to_master(Binlog_socket *binlog_socket,
                                 boost::asio::streambuf &server_messages,
                                 const std::string& host, long port)
{
  std::ostream command_request_stream(&server_messages);

  boost::uint8_t val_command = COM_REGISTER_SLAVE;
  Protocol_chunk<boost::uint8_t> prot_command(val_command);
  boost::uint16_t val_port = port;
  Protocol_chunk<boost::uint16_t> prot_connection_port(val_port);
  boost::uint32_t val_rpl_recovery_rank = 0;
  Protocol_chunk<boost::uint32_t> prot_rpl_recovery_rank(val_rpl_recovery_rank);
  boost::uint32_t val_server_id = 1;
  Protocol_chunk<boost::uint32_t> prot_server_id(val_server_id);

  const char* env_libreplication_server_id = std::getenv("LIBREPLICATION_SERVER_ID");

  if (env_libreplication_server_id != 0) {
    try {
      boost::uint32_t libreplication_server_id = boost::lexical_cast<boost::uint32_t>(env_libreplication_server_id);
      prot_server_id = libreplication_server_id;
    } catch (boost::bad_lexical_cast e) {
      // XXX: nothing to do
    }
  }

  boost::uint32_t val_master_server_id = 0;
  Protocol_chunk<boost::uint32_t> prot_master_server_id(val_master_server_id);

  boost::uint8_t val_report_host_strlen = host.size();
  Protocol_chunk<boost::uint8_t> prot_report_host_strlen(val_report_host_strlen);
  std::string report_user("mrl_user");
  boost::uint8_t val_user_strlen = report_user.size();
  Protocol_chunk<boost::uint8_t> prot_user_strlen(val_user_strlen);
  std::string report_passwd("pw");
  boost::uint8_t val_passwd_strlen = report_passwd.size();
  Protocol_chunk<boost::uint8_t> prot_passwd_strlen(val_passwd_strlen);

  command_request_stream << prot_command
          << prot_server_id
          << prot_report_host_strlen
          << host
          << prot_user_strlen
          << report_user
          << prot_passwd_strlen
          << report_passwd
          << prot_connection_port
          << prot_rpl_recovery_rank
          << prot_master_server_id;

  try {
    // Send request.
    write_request(binlog_socket, server_messages, binlog_socket->reset_and_increment_packet_number());
  } catch( boost::system::error_code e)
  {
    throw std::runtime_error("Boost system error: " + e.message());
  }


  // Get Ok-package
  unsigned long packet_length;
  unsigned char packet_no;
  packet_length=proto_get_one_package(binlog_socket, server_messages, &packet_no);

  std::istream cmd_response_stream(&server_messages);

  boost::uint8_t result_type;
  Protocol_chunk<boost::uint8_t> prot_result_type(result_type);

  cmd_response_stream >> prot_result_type;


  if (result_type == 0)
  {
    struct st_ok_package ok_package;
    prot_parse_ok_message(cmd_response_stream, ok_package, packet_length);
  } else
  {
    struct st_error_package error_package;
    prot_parse_error_message(cmd_response_stream, error_package, packet_length);
    throw std::runtime_error("Error from server, code=" + boost::lexical_cast<std::string>(error_package.error_code) + ", message=\"" + error_package.message + "\"");
  }
  return 0;
}

    int Binlog_tcp_driver::wait_for_next_event(mysql::Binary_log_event **event_ptr)
{
  boost::mutex::scoped_lock lock(m_event_queue_pop_back_mutex);

  if (m_shutdown) {
    return ERR_EOF;
  }
  // poll for new event until one event is found.
  // return the event
  if (event_ptr)
    *event_ptr= 0;
  m_event_queue->pop_back(event_ptr);
  if (event_ptr && *event_ptr == 0) {
    return ERR_EOF;
  }
  if (Incident_event *incident = dynamic_cast<Incident_event*>(*event_ptr)) {
    if (incident->type == PACKET_READ_ERROR) {
      delete incident;
      throw std::runtime_error("Error reading data from MySQL server");
    }
  }
  return 0;
}

void Binlog_tcp_driver::start_event_loop()
{
  try {
    boost::system::error_code err;
    int executed_jobs=m_io_service.run(err);
    if (err)
    {
      // TODO what error appear here?
    }

    /*
      This function must be called prior to any second or later set of
      invocations of the run(), run_one(), poll() or poll_one() functions when
      a previous invocation of these functions returned due to the io_service
      being stopped or running out of work. This function allows the io_service
      to reset any internal state, such as a "stopped" flag.
    */
    m_io_service.reset();

    /*
      Don't shutdown until the io service has reset!
    */
    if (m_shutdown)
    {
      m_shutdown= false;
    } else {
      std::cerr << "the event loop finished unexpectedly\n";
    }
  } catch (const std::exception& e) {
    std::cerr << "error in the event loop: " << e.what() << "\n";
  }
}

int Binlog_tcp_driver::connect()
{
  return connect(m_user, m_passwd, m_host, m_port, m_binlog_file_name, m_binlog_offset);
}

/**
 * Make synchronous reconnect.
 */
void Binlog_tcp_driver::reconnect()
{
  disconnect();
  connect(m_user, m_passwd, m_host, m_port);
}

int Binlog_tcp_driver::disconnect()
{
  if (m_socket == 0) {
    return ERR_OK;
  }

  m_shutdown= true;

  if (!m_event_queue->has_unread()) {
    m_event_queue->push_front(0);  // push EOF event
  }

  /*
    By posting to the io service we guarantee that the operations are
    executed in the same thread as the io_service is running in.
  */
  // Post shutdown, which stops the io_service
  m_io_service.post(boost::bind(&Binlog_tcp_driver::shutdown, this));
  // Consume events in the queue.  Without this, a net packet handler may
  // get stuck waiting for the full buffer to have an empty slot.
  drain_event_queue();

  m_waiting_event= 0;
  // Wait for the event loop thread to stop
  if (m_event_loop)
  {
    m_event_loop->join();
    delete m_event_loop;
    m_event_loop= 0;
  }
  // Consume the stream buffer
  m_event_stream_buffer.consume(m_event_stream_buffer.in_avail());
  // Consume the event queue to delete event(s) which may have
  //    been pushed while waiting for the event loop to finish.
  drain_event_queue();
  // Clean up the socket
  m_socket->close();
  delete m_socket;
  m_socket= 0;
  return ERR_OK;
}


void Binlog_tcp_driver::shutdown(void)
{
  m_io_service.stop();
}

int Binlog_tcp_driver::set_position(const std::string &str, unsigned long position)
{
  /*
    Disconnect the current connection before validating the new connection.
    Otherwise, a temporary connection for the validation lets the existing
    one hang.
  */
  disconnect();

  /*
    Validate the new position before we attempt to set. Once we set the
    position we won't know if it succeded because the binlog dump is
    running in another thread asynchronously.
  */
  boost::asio::io_service io_service;
  Binlog_socket *binlog_socket;

  if ((binlog_socket= sync_connect_and_authenticate(io_service, m_user, m_passwd, m_host, m_port)) == 0) {
    throw std::runtime_error("Connect or authentication error");
  }

  std::map<std::string, unsigned long > binlog_map;
  fetch_binlogs_name_and_size(binlog_socket, binlog_map);
  binlog_socket->close();
  delete binlog_socket;

  std::map<std::string, unsigned long >::iterator binlog_itr= binlog_map.find(str);

  bool is_valid_position = true;
  std::string err_message = "";
  /*
    If the file name isn't listed on the server we will fail here.
  */
  if (binlog_itr == binlog_map.end()) {
    err_message = "binlog file " + str + " does not exist on MySQL server";
    is_valid_position = false;
  }

  /*
    If the requested position is greater than the file size we will fail
    here.
  */
  if (position > binlog_itr->second) {
    err_message = "requested binlog position " + boost::lexical_cast<std::string>(position)
                  + " is larger than the binlog file size (" + binlog_itr->first + ":"
                  + boost::lexical_cast<std::string>(binlog_itr->second) + ") on MySQL server";
    is_valid_position = false;
  }

  /*
    Uppon return of connect we only know if we succesfully authenticated
    against the server. The binlog dump command is executed asynchronously
    in another thread.
  */

  int result = -1;
  if (is_valid_position) {
    result = connect(m_user, m_passwd, m_host, m_port, str, position);
  } else {
    throw std::runtime_error(err_message);
  }
  if (is_valid_position && result == 0) {
    return ERR_OK;
  } else {
    return ERR_FAIL;
  }
}

int Binlog_tcp_driver::get_position(std::string *filename_ptr, unsigned long *position_ptr)
{
  boost::asio::io_service io_service;

  Binlog_socket *binlog_socket;

  if ((binlog_socket=sync_connect_and_authenticate(io_service, m_user, m_passwd, m_host, m_port)) == 0)
    return ERR_FAIL;

  if (fetch_master_status(binlog_socket, &m_binlog_file_name, &m_binlog_offset))
    return ERR_FAIL;

  binlog_socket->close();
  delete binlog_socket;
  if (filename_ptr)
    *filename_ptr= m_binlog_file_name;
  if (position_ptr)
    *position_ptr= m_binlog_offset;
  return ERR_OK;
}

bool fetch_master_status(Binlog_socket *binlog_socket, std::string *filename, unsigned long *position)
{
  // Command body
  boost::asio::streambuf server_messages;
  std::ostream command_request_stream(&server_messages);

  boost::uint8_t val_command = COM_QUERY;
  Protocol_chunk<boost::uint8_t> prot_command(val_command);

  command_request_stream << prot_command
          << "SHOW MASTER STATUS";

  // Send request
  write_request(binlog_socket, server_messages, binlog_socket->reset_and_increment_packet_number());

  // Get response
  Result_set result_set(binlog_socket);

  Converter conv;
  BOOST_FOREACH(Row_of_fields row, result_set)
  {
    *filename= "";
    conv.to(*filename, row[0]);
    long pos;
    conv.to(pos, row[1]);
    *position= (unsigned long)pos;
  }
  return false;
}

bool fetch_binlogs_name_and_size(Binlog_socket *binlog_socket, std::map<std::string, unsigned long> &binlog_map)
{
  boost::asio::streambuf server_messages;

  std::ostream command_request_stream(&server_messages);

  boost::uint8_t val_command = COM_QUERY;
  Protocol_chunk<boost::uint8_t> prot_command(val_command);

  command_request_stream << prot_command
          << "SHOW BINARY LOGS";

  // Send request
  write_request(binlog_socket, server_messages, binlog_socket->reset_and_increment_packet_number());

  // Get response
  Result_set result_set(binlog_socket);

  Converter conv;
  BOOST_FOREACH(Row_of_fields row, result_set)
  {
    std::string filename;
    long position;
    conv.to(filename, row[0]);
    conv.to(position, row[1]);
    binlog_map.insert(std::make_pair<std::string, unsigned long>(filename, (unsigned long)position));
  }
  return false;
}

bool set_master_binlog_checksum(Binlog_socket *binlog_socket, bool &checksum_aware_master)
{
  boost::asio::streambuf server_messages;

  std::ostream command_request_stream(&server_messages);

  boost::uint8_t val_command = COM_QUERY;
  Protocol_chunk<boost::uint8_t> prot_command(val_command);

  command_request_stream << prot_command
          << "SET @master_binlog_checksum=@@global.binlog_checksum";

  // Send request
  write_request(binlog_socket, server_messages, binlog_socket->reset_and_increment_packet_number());

  // Get Ok-package
  unsigned long packet_length;
  unsigned char packet_no;
  packet_length=proto_get_one_package(binlog_socket, server_messages, &packet_no);

  std::istream cmd_response_stream(&server_messages);

  boost::uint8_t result_type;
  Protocol_chunk<boost::uint8_t> prot_result_type(result_type);

  cmd_response_stream >> prot_result_type;


  if (result_type == 0)
  {
    struct st_ok_package ok_package;
    prot_parse_ok_message(cmd_response_stream, ok_package, packet_length);
    checksum_aware_master = true;
  } else
  {
    checksum_aware_master = false;
    struct st_error_package error_package;
    prot_parse_error_message(cmd_response_stream, error_package, packet_length);
    if (error_package.error_code == 1193) {// ER_UNKNOWN_SYSTEM_VARIABLE
      // The master does not know about checksum.  No need to throw an
      // exception.
    } else {
      throw std::runtime_error("Error from server, code=" + boost::lexical_cast<std::string>(error_package.error_code) + ", message=\"" + error_package.message + "\"");
    }
  }
  return false;
}

bool fetch_master_binlog_checksum(Binlog_socket *binlog_socket, boost::uint8_t &checksum_alg)
{
  boost::asio::streambuf server_messages;

  std::ostream command_request_stream(&server_messages);

  boost::uint8_t val_command = COM_QUERY;
  Protocol_chunk<boost::uint8_t> prot_command(val_command);

  command_request_stream << prot_command
          << "SELECT @master_binlog_checksum";

  // Send request
  write_request(binlog_socket, server_messages, binlog_socket->reset_and_increment_packet_number());

  // Get response
  Result_set result_set(binlog_socket);

  Converter conv;
  BOOST_FOREACH(Row_of_fields row, result_set)
  {
    std::string checksum_type_name;
    conv.to(checksum_type_name, row[0]);

    if (checksum_type_name == "NONE")
      checksum_alg = BINLOG_CHECKSUM_ALG_OFF;
    else if (checksum_type_name == "CRC32")
      checksum_alg = BINLOG_CHECKSUM_ALG_CRC32;
    else
      checksum_alg = BINLOG_CHECKSUM_ALG_UNDEF;
  }
  return false;
}


#define SCRAMBLE_BUFF_SIZE 20

int hash_sha1(boost::uint8_t *output, ...)
{
  /* size at least EVP_MAX_MD_SIZE */
  va_list ap;
  size_t result;
  EVP_MD_CTX *hash_context = EVP_MD_CTX_create();

  va_start(ap, output);
  EVP_DigestInit_ex(hash_context, EVP_sha1(), NULL);
  while ( 1 )
  {
    const boost::uint8_t *data = va_arg(ap, const boost::uint8_t *);
    int length = va_arg(ap, int);
    if ( length < 0 )
      break;
    EVP_DigestUpdate(hash_context, data, length);
  }
  EVP_DigestFinal_ex(hash_context, (unsigned char *)output, (unsigned int *)&result);
  va_end(ap);
  return result;
}


int encrypt_password(boost::uint8_t *reply,   /* buffer at least EVP_MAX_MD_SIZE */
	                   const boost::uint8_t *scramble_buff,
		                 const char *pass)
{
  boost::uint8_t hash_stage1[EVP_MAX_MD_SIZE], hash_stage2[EVP_MAX_MD_SIZE];
  //EVP_MD_CTX *hash_context = EVP_MD_CTX_create();

  /* Hash password into hash_stage1 */
  int length_stage1 = hash_sha1(hash_stage1,
                                pass, strlen(pass),
                                NULL, -1);

  /* Hash hash_stage1 into hash_stage2 */
  int length_stage2 = hash_sha1(hash_stage2,
                                hash_stage1, length_stage1,
                                NULL, -1);

  int length_reply = hash_sha1(reply,
                               scramble_buff, SCRAMBLE_BUFF_SIZE,
                               hash_stage2, length_stage2,
                               NULL, -1);

  //assert(length_reply <= EVP_MAX_MD_SIZE);
  //assert(length_reply == length_stage1);

  int i;
  for ( i=0 ; i<length_reply ; ++i )
    reply[i] = hash_stage1[i] ^ reply[i];
  return length_reply;
}

int Binlog_tcp_driver::set_ssl_ca(const std::string& filepath)
{
  m_opt_ssl_ca= filepath;
  return ERR_OK;
}

int Binlog_tcp_driver::set_ssl_cipher(const std::string& cipher_list)
{
  m_opt_ssl_cipher= cipher_list;
  return ERR_OK;
}

void Binlog_tcp_driver::drain_event_queue()
{
  boost::mutex::scoped_lock lock(m_event_queue_pop_back_mutex);

  Binary_log_event *event;
  while(m_event_queue->has_unread())
  {
    m_event_queue->pop_back(&event);
    delete event;
  }
}

}} // end namespace mysql::system
