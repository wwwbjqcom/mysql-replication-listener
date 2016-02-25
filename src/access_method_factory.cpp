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
#include "access_method_factory.h"
#include "tcp_driver.h"
#include "file_driver.h"

using mysql::system::Binary_log_driver;
using mysql::system::Binlog_tcp_driver;
using mysql::system::Binlog_file_driver;

/*
 * The following code snippet is from:
 * http://www.codeguru.com/cpp/cpp/algorithms/strings/article.php/c12759/URI-Encoding-and-Decoding.htm
 */
const char HEX2DEC[256] =

{

    /*       0  1  2  3   4  5  6  7   8  9  A  B   C  D  E  F */

    /* 0 */ -1,-1,-1,-1, -1,-1,-1,-1, -1,-1,-1,-1, -1,-1,-1,-1,

    /* 1 */ -1,-1,-1,-1, -1,-1,-1,-1, -1,-1,-1,-1, -1,-1,-1,-1,

    /* 2 */ -1,-1,-1,-1, -1,-1,-1,-1, -1,-1,-1,-1, -1,-1,-1,-1,

    /* 3 */  0, 1, 2, 3,  4, 5, 6, 7,  8, 9,-1,-1, -1,-1,-1,-1,



    /* 4 */ -1,10,11,12, 13,14,15,-1, -1,-1,-1,-1, -1,-1,-1,-1,

    /* 5 */ -1,-1,-1,-1, -1,-1,-1,-1, -1,-1,-1,-1, -1,-1,-1,-1,

    /* 6 */ -1,10,11,12, 13,14,15,-1, -1,-1,-1,-1, -1,-1,-1,-1,

    /* 7 */ -1,-1,-1,-1, -1,-1,-1,-1, -1,-1,-1,-1, -1,-1,-1,-1,



    /* 8 */ -1,-1,-1,-1, -1,-1,-1,-1, -1,-1,-1,-1, -1,-1,-1,-1,

    /* 9 */ -1,-1,-1,-1, -1,-1,-1,-1, -1,-1,-1,-1, -1,-1,-1,-1,

    /* A */ -1,-1,-1,-1, -1,-1,-1,-1, -1,-1,-1,-1, -1,-1,-1,-1,

    /* B */ -1,-1,-1,-1, -1,-1,-1,-1, -1,-1,-1,-1, -1,-1,-1,-1,



    /* C */ -1,-1,-1,-1, -1,-1,-1,-1, -1,-1,-1,-1, -1,-1,-1,-1,

    /* D */ -1,-1,-1,-1, -1,-1,-1,-1, -1,-1,-1,-1, -1,-1,-1,-1,

    /* E */ -1,-1,-1,-1, -1,-1,-1,-1, -1,-1,-1,-1, -1,-1,-1,-1,

    /* F */ -1,-1,-1,-1, -1,-1,-1,-1, -1,-1,-1,-1, -1,-1,-1,-1

};

std::string UriDecode(const std::string & sSrc)

{

    // Note from RFC1630:  "Sequences which start with a percent sign

    // but are not followed by two hexadecimal characters (0-9, A-F) are reserved

    // for future extension"



    const unsigned char * pSrc = (const unsigned char *)sSrc.c_str();

	const int SRC_LEN = sSrc.length();

    const unsigned char * const SRC_END = pSrc + SRC_LEN;

    const unsigned char * const SRC_LAST_DEC = SRC_END - 2;   // last decodable '%'



    char * const pStart = new char[SRC_LEN];

    char * pEnd = pStart;



    while (pSrc < SRC_LAST_DEC)

	{

		if (*pSrc == '%')

        {

            char dec1, dec2;

            if (-1 != (dec1 = HEX2DEC[*(pSrc + 1)])

                && -1 != (dec2 = HEX2DEC[*(pSrc + 2)]))

            {

                *pEnd++ = (dec1 << 4) + dec2;

                pSrc += 3;

                continue;

            }

        }



        *pEnd++ = *pSrc++;

	}



    // the last 2- chars

    while (pSrc < SRC_END)

        *pEnd++ = *pSrc++;



    std::string sResult(pStart, pEnd);

    delete [] pStart;

	return sResult;

}


/**
   Parse the body of a MySQL URI.

   The format is <code>user[:password]@host[:port]</code>
*/
static Binary_log_driver *parse_mysql_url(const char *body, size_t len)
{
  /* Find the beginning of the user name */
  if (strncmp(body, "//", 2) != 0)
    return 0;

  /* Find the user name, which is mandatory */
  const char *user = body + 2;
  const char *user_end= strpbrk(user, ":@");
  if (user_end == 0 || user_end == user)
    return 0;
  assert(user_end - user >= 1);          // There has to be a username

  /* Find the password, which can be empty */
  assert(*user_end == ':' || *user_end == '@');
  const char *const pass = user_end + 1;        // Skip the ':' (or '@')
  const char *pass_end = pass;
  if (*user_end == ':')
  {
    pass_end = strchr(pass, '@');
    if (pass_end == 0)
      return 0;       // There should be a password, but '@' was not found
  }
  assert(pass_end - pass >= 0);               // Password can be empty

  /* Find the host name, which is mandatory */
  // Skip the '@', if there is one
  const char *host = *pass_end == '@' ? pass_end + 1 : pass_end;
  const char *host_end = strchr(host, ':');
  if (host == host_end)
    return 0;                                 // No hostname was found
  /* If no ':' was found there is no port, so the host end at the end
   * of the string */
  if (host_end == 0)
    host_end = body + len;
  assert(host_end - host >= 1);              // There has to be a host

  /* Find the port number */
  unsigned long portno = 3306;
  if (*host_end == ':')
    portno = strtoul(host_end + 1, NULL, 10);
  std::string user_str = UriDecode(std::string(user, user_end - user));
  std::string pass_str = UriDecode(std::string(pass, pass_end - pass));
  std::string host_str = UriDecode(std::string(host, host_end - host));

  /* Host name is now the string [host, port-1) if port != NULL and [host, EOS) otherwise. */
  /* Port number is stored in portno, either the default, or a parsed one */
  return new Binlog_tcp_driver(user_str, pass_str, host_str, portno);
}


static Binary_log_driver *parse_file_url(const char *body, size_t length)
{
  /* Find the beginning of the file name */
  if (strncmp(body, "//", 2) != 0)
    return 0;

  /*
    Since we don't support host information yet, there should be a
    slash after the initial "//".
   */
  if (body[2] != '/')
    return 0;

  return new Binlog_file_driver(body + 2);
}

/**
   URI parser information.
 */
struct Parser {
  const char* protocol;
  Binary_log_driver *(*parser)(const char *body, size_t length);
};

/**
   Array of schema names and matching parsers.
*/
static Parser url_parser[] = {
  { "mysql", parse_mysql_url },
  { "file",  parse_file_url },
};

Binary_log_driver *
mysql::system::create_transport(const char *url)
{
  const char *pfx = strchr(url, ':');
  if (pfx == 0)
    return NULL;
  for (int i = 0 ; i < sizeof(url_parser)/sizeof(*url_parser) ; ++i)
  {
    const char *proto = url_parser[i].protocol;
    if (strncmp(proto, url, strlen(proto)) == 0)
      return (*url_parser[i].parser)(pfx+1, strlen(pfx+1));
  }
  return NULL;
}
