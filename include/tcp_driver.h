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

#ifndef _TCP_DRIVER_H
#define	_TCP_DRIVER_H
#include "binlog_driver.h"
#include "bounded_buffer.h"
#include "protocol.h"
#include <boost/asio.hpp>
#include <boost/thread.hpp>
#include "binlog_socket.h"


#define MAX_PACKAGE_SIZE 0xffffff

#define GET_NEXT_PACKET_HEADER   \
   m_socket->async_read(boost::asio::buffer(m_net_header, 4), \
     boost::bind(&Binlog_tcp_driver::handle_net_packet_header, this, \
     boost::asio::placeholders::error, boost::asio::placeholders::bytes_transferred)) \

using boost::asio::ip::tcp;

namespace mysql { namespace system {

class Binlog_tcp_driver : public Binary_log_driver
{
public:

    Binlog_tcp_driver(const std::string& user, const std::string& passwd,
                      const std::string& host, unsigned long port,
                      const std::string& binlog_file,
                      unsigned long binlog_offset)
      : Binary_log_driver(binlog_file, binlog_offset),
        m_host(host), m_user(user), m_passwd(passwd), m_port(port),
        m_socket(NULL), m_waiting_event(0), m_event_loop(0),
        m_total_bytes_transferred(0), m_shutdown(false),
        m_event_queue(new bounded_buffer<Binary_log_event*>(50))
    {
    }

    ~Binlog_tcp_driver()
    {
      disconnect();
      delete m_event_queue;
    }

    /**
     * Connect using previously declared connection parameters.
     */
    int connect();

    /**
     * Blocking wait for the next binary log event to reach the client
     */
    int wait_for_next_event(mysql::Binary_log_event **event);

    /**
     * Reconnects to the master with a new binlog dump request.
     */
    int set_position(const std::string &str, unsigned long position);

    int get_position(std::string *str, unsigned long *position);

    int set_ssl_ca(const std::string& filepath);
    int set_ssl_cipher(const std::string& cipher_list);

    const std::string& user() const { return m_user; }
    const std::string& password() const { return m_passwd; }
    const std::string& host() const { return m_host; }
    unsigned long port() const { return m_port; }

protected:
    /**
     * Connects to a mysql server, authenticates and initiates the event
     * request loop.
     *
     * @param user The user account on the server side
     * @param passwd The password used to authenticate the user
     * @param host The DNS host name or IP of the server
     * @param port The service port number to connect to
     *
     *
     * @return Success or failure code
     *   @retval 0 Successfully established a connection
     *   @retval >1 An error occurred.
     */
    int connect(const std::string& user, const std::string& passwd,
                const std::string& host, long port,
                const std::string& binlog_filename="", size_t offset=4);

private:

    /**
     * Request a binlog dump and starts the event loop in a new thread
     * @param binlog_file_name The base name of the binlog files to query
     *
     */
    void start_binlog_dump(const std::string &binlog_file_name, size_t offset);

    /**
     * Stop the event loop thread
     */
    void stop_binlog_dump();

    /**
     * Handles a completed mysql server package header and put a
     * request for the body in the job queue.
     */
    void handle_net_packet_header(const boost::system::error_code& err, std::size_t bytes_transferred);

    /**
     * Handles a completed network package with the assumption that it contains
     * a binlog event.
     *
     * TODO rename to handle_event_log_packet?
     */
    void handle_net_packet(const boost::system::error_code& err, std::size_t bytes_transferred);

    /**
     * Called from handle_net_packet(). The function handle a stream of bytes
     * representing event packets which may or may not be complete.
     * It uses m_waiting_event and the size of the stream as parameters
     * in a state machine. If there is no m_waiting_event then the event
     * header must be parsed for the event packet length. This can only
     * be done if the accumulated stream of bytes are more than 19.
     * Next, if there is a m_waiting_event, it can only be completed if
     * event_length bytes are waiting on the stream.
     *
     * If none of these conditions are fullfilled, the function exits without
     * any action.
     *
     * @param err Not used
     * @param bytes_transferred The number of bytes waiting in the event stream
     *
     */
    void handle_event_packet(const boost::system::error_code& err, std::size_t bytes_transferred);

    /**
     * Executes io_service in a loop.
     * TODO Checks for connection errors and reconnects to the server
     * if necessary.
     */
    void start_event_loop(void);

    /**
     * Reconnect to the server by first calling disconnect and then connect.
     */
    void reconnect(void);

    /**
     * Disconnet from the server. The io service must have been stopped before
     * this function is called.
     * The event queue is emptied.
     */
    void disconnect(void);

    /**
     * Terminates the io service and sets the shudown flag.
     * this causes the event loop to terminate.
     */
    void shutdown(void);


    /**
     * Connect mysql server and authenticate.
     * This process includes SSL handshaking.
     */
    Binlog_socket* sync_connect_and_authenticate(boost::asio::io_service &io_service,
                                                 const std::string &user,
                                                 const std::string &passwd,
                                                 const std::string &host,
                                                 long port);


    boost::thread *m_event_loop;
    boost::asio::io_service m_io_service;
    Binlog_socket *m_socket;
    bool m_shutdown;

    /**
     * Temporary storage for a handshake package
     */
    st_handshake_package m_handshake_package;

    /**
     * Temporary storage for an OK package
     */
    st_ok_package m_ok_package;

    /**
     * Temporary storage for an error package
     */
    st_error_package m_error_package;

    /**
     * each bin log event starts with a 19 byte long header
     * We use this sturcture every time we initiate an async
     * read.
     */
    boost::uint8_t m_event_header[19];

    /**
     *
     */
    boost::uint8_t m_net_header[4];

    /**
     *
     */
    boost::uint8_t m_net_packet[MAX_PACKAGE_SIZE];
    boost::asio::streambuf m_event_stream_buffer;
    char * m_event_packet;

    /**
     * This pointer points to an object constructed from event
     * stream during async communication with
     * server. If it is 0 it means that no event has been
     * constructed yet.
     */
    Log_event_header *m_waiting_event;
    Log_event_header m_log_event_header;
    /**
     * A ring buffer used to dispatch aggregated events to the user application
     */
    bounded_buffer<Binary_log_event *> *m_event_queue;

    std::string m_user;
    std::string m_host;
    std::string m_passwd;
    long m_port;

    boost::uint64_t m_total_bytes_transferred;


    /*
     * SSL configuration
     */
    //bool m_opt_use_ssl;
    std::string m_opt_ssl_ca;
    //std::string m_opt_ssl_capath;
    //std::string m_opt_ssl_cert;
    std::string m_opt_ssl_cipher;
    //std::string m_opt_ssl_key;
    //std::string m_opt_ssl_crl;
    //std::string m_opt_ssl_crlpath;
    //bool m_opt_ssl_verify_server_cert;
};

/*
 * Start ssl handshaking with mysql server
 */
void start_ssl(Binlog_socket *binlog_socket, struct st_handshake_package &handshake_package);

/*
 * Send authenticate request and handle the response
 */
int authenticate(Binlog_socket *socket, const std::string& user,
    const std::string& passwd,
    const st_handshake_package &handshake_package);

/*
 * Register slave to master
 */
int register_slave_to_master(Binlog_socket *binlog_socket,
    boost::asio::streambuf &server_messages,
    const std::string& host, long port);

/**
 * Write a request with header packet
 * @param binlog_socket
 * @param request_body_buf buffer content for sending to mysql-server
 * @param packet_number Packet number in header data.
 */
std::size_t write_request(Binlog_socket *binlog_socket,
    boost::asio::streambuf &request_body_buf,
    std::size_t packet_number);

/**
 * Write a request with header packet
 * packet number will be set automatically
 * @param binlog_socket
 * @param request_body_buf buffer content for sending to mysql-server
 */
std::size_t write_request(Binlog_socket *binlog_socket,
    boost::asio::streambuf &request_body_buf);

/**
 * Sends a SHOW MASTER STATUS command to the server and retrieve the
 * current binlog position.
 *
 * @return False if the operation succeeded, true if it failed.
 */
bool fetch_master_status(Binlog_socket *binlog_socket, std::string *filename, unsigned long *position);
/**
 * Sends a SHOW BINARY LOGS command to the server and stores the file
 * names and sizes in a map.
 */
bool fetch_binlogs_name_and_size(Binlog_socket *binlog_socket, std::map<std::string, unsigned long> &binlog_map);
/**
 * Sends a "SET @master_binlog_checksum=..." command to the server in order to
 * notify that the slave is aware of checksum.
 */
bool set_master_binlog_checksum(Binlog_socket *binlog_socket, bool &checksum_aware_master);

/**
 * Fetch the master's binlog checksum type
 */
bool fetch_master_binlog_checksum(Binlog_socket *binlog_socket, boost::uint8_t &checksum_alg);

} }
#endif	/* _TCP_DRIVER_H */
