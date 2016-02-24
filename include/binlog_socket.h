#ifndef _BINLOG_SOCKET_H
#define _BINLOG_SOCKET_H

#include <boost/asio.hpp>
#include <boost/asio/ssl.hpp>

using boost::asio::ip::tcp;

namespace mysql {
namespace system {


class Binlog_socket {
public:
  Binlog_socket(boost::asio::io_service& io_service)
    : m_ssl_flag(false), m_handshake_flag(false), m_packet_number(0)
  {
    m_socket = new boost::asio::ip::tcp::socket(io_service);
    m_ssl_socket = NULL;
    m_ssl_context = NULL;
  }

  Binlog_socket(boost::asio::io_service& io_service, boost::asio::ssl::context *ssl_context)
    : m_ssl_flag(true), m_handshake_flag(false), m_packet_number(0)
  {
    m_socket = new boost::asio::ip::tcp::socket(io_service);
    m_ssl_socket = new boost::asio::ssl::stream<boost::asio::ip::tcp::socket&>(*m_socket, *ssl_context);
    m_ssl_context = ssl_context;
  }

  ~Binlog_socket()
  {
    if (m_socket)
      delete m_socket;
    if (m_ssl_socket)
      delete m_ssl_socket;
    if (m_ssl_context)
      delete m_ssl_context;
  }

  boost::asio::ip::tcp::socket* socket()
  {
    return m_socket;
  }

  bool is_open()
  {
    return m_socket->is_open();
  }

  void close()
  {
    m_socket->close();
  }

  void cancel()
  {
    m_socket->cancel();
  }

  boost::asio::io_service &get_io_service()
  {
    m_socket->get_io_service();
  }

  bool is_ssl()
  {
    return m_ssl_flag;
  }

  bool has_handshaken()
  {
    return m_handshake_flag;
  }

  bool should_use_ssl()
  {
    return m_ssl_flag && m_handshake_flag;
  }

  void handshake()
  {
    if (is_ssl())
    {
      m_ssl_socket->handshake(boost::asio::ssl::stream_base::client);
      m_handshake_flag = true;
    }
  }

  void set_ssl_cipher(std::string& cipher_list)
  {
    if (is_ssl())
    {
#if (BOOST_VERSION / 100000 >= 1) && (BOOST_VERSION / 100 % 1000 >= 47)
      SSL_set_cipher_list(m_ssl_socket->native_handle(), cipher_list.c_str());
#else
#warning "set_ssl_cipher api is disabled. Boost version needs to be 1.47 or later."
#endif
    }
  }

  /**
   * Packet number (for sequence id of mysql packet)
   * http://dev.mysql.com/doc/internals/en/mysql-packet.html
   */

  std::size_t get_and_increment_packet_number()
  {
    return m_packet_number++;
  }

  std::size_t reset_and_increment_packet_number()
  {
    m_packet_number = 1;
    return 0;
  }

  std::size_t packet_number()
  {
    return m_packet_number;
  }

  void set_packet_number(std::size_t new_num)
  {
    m_packet_number = new_num;
  }


  /*
   * read methods (forward to read.hpp)
   */

  template <typename MutableBufferSequence, typename CompletionCondition>
  std::size_t read(const MutableBufferSequence& buffers, CompletionCondition completion_condition)
  {
    if (should_use_ssl())
      return boost::asio::read(*m_ssl_socket, buffers, completion_condition);
    else
      return boost::asio::read(*m_socket, buffers, completion_condition);
  }

  template <typename Allocator, typename CompletionCondition>
  std::size_t read(boost::asio::basic_streambuf<Allocator>& b, CompletionCondition completion_condition)
  {
    if (should_use_ssl())
      return boost::asio::read(*m_ssl_socket, b, completion_condition);
    else
      return boost::asio::read(*m_socket, b, completion_condition);
  }

  template <typename MutableBufferSequence, typename ReadHandler>
  void async_read(const MutableBufferSequence& buffers, ReadHandler handler)
  {
    if (should_use_ssl())
      boost::asio::async_read(*m_ssl_socket, buffers, handler);
    else
      boost::asio::async_read(*m_socket, buffers, handler);
  }

  template <typename Allocator, typename ReadHandler>
  void async_read(boost::asio::basic_streambuf<Allocator>& b, ReadHandler handler)
  {
    if (should_use_ssl())
      boost::asio::async_read(*m_ssl_socket, b, handler);
    else
      boost::asio::async_read(*m_socket, b, handler);
  }


  /*
   * write methods (forward to write.hpp)
   */

  template <typename ConstBufferSequence>
  std::size_t write(const ConstBufferSequence& buffers)
  {
    if (should_use_ssl())
      return boost::asio::write(*m_ssl_socket, buffers);
    else
      return boost::asio::write(*m_socket, buffers);
  }

  template <typename ConstBufferSequence>
  std::size_t write(const ConstBufferSequence& buffers, boost::system::error_code& ec)
  {
    if (should_use_ssl())
      return boost::asio::write(*m_ssl_socket, buffers, ec);
    else
      return boost::asio::write(*m_socket, buffers, ec);
  }

  template <typename ConstBufferSequence, typename CompletionCondition>
  std::size_t write(const ConstBufferSequence& buffers, CompletionCondition completion_condition)
  {
    if (should_use_ssl())
      return boost::asio::write(*m_ssl_socket, buffers, completion_condition);
    else
      return boost::asio::write(*m_socket, buffers, completion_condition);
  }

  template <typename Allocator>
  std::size_t write(boost::asio::basic_streambuf<Allocator>& b)
  {
    if (should_use_ssl())
      return boost::asio::write(*m_ssl_socket, b);
    else
      return boost::asio::write(*m_socket, b);
  }

  template <typename Allocator>
  std::size_t write(boost::asio::basic_streambuf<Allocator>& b, boost::system::error_code& ec)
  {
    if (should_use_ssl())
      return boost::asio::write(*m_ssl_socket, b, ec);
    else
      return boost::asio::write(*m_socket, b, ec);
  }

  template <typename Allocator, typename CompletionCondition>
  std::size_t write(boost::asio::basic_streambuf<Allocator>& b, CompletionCondition completion_condition)
  {
    if (should_use_ssl())
      return boost::asio::write(*m_ssl_socket, b, completion_condition);
    else
      return boost::asio::write(*m_socket, b, completion_condition);
  }

private:

  bool m_ssl_flag;
  bool m_handshake_flag;
  std::size_t m_packet_number;  // for request header
  boost::asio::ip::tcp::socket *m_socket;
  boost::asio::ssl::stream<boost::asio::ip::tcp::socket&> *m_ssl_socket;
  boost::asio::ssl::context *m_ssl_context;
};

} // end namespace system
} // end namespace mysql

#endif /* _BINLOG_SOCKET_H */
