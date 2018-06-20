/**
 * @file   url.cc
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2018 University of California, Los Angeles and Intel Corporation
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in
 * all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
 * THE SOFTWARE.
 *
 * @section DESCRIPTION
 *  
 * URL Parsing
 */

#include "url.h"
#include <stdint.h>
#include <string>
#include <algorithm>
#include <cctype>
#include <functional>
#include <system_error>

using namespace std;

// Constructor
url::url(const std::string& url_s) {
  parse(url_s);
}

// Accessors
std::string url::protocol() {
  return protocol_;
}

std::string url::host() {
  return host_;
}

std::string url::port() {
  return port_;
}

int16_t url::nport() {
  return nport_;
}

std::string url::path() {
  return path_;
}

std::string url::query() {
  return query_;
}

// Private Methods
void url::parse(const string& url_s)
{
  if (url_s.empty()) {
    throw system_error(EINVAL, std::generic_category(), "Cannot parse empty string as an URL");
  }
  
  const string::const_iterator start_iter = url_s.begin();
  const string::const_iterator end_iter = url_s.end();
  
  const string protocol_end("://");
  string::const_iterator protocol_iter = search(start_iter, end_iter, protocol_end.begin(), protocol_end.end());
  if (protocol_iter == url_s.end()) {
    throw system_error(EINVAL, std::generic_category(), "String does not seem to be a URL");
  }

  // protocol is case insensitive
  protocol_.reserve(distance(start_iter, protocol_iter));
  transform(start_iter, protocol_iter, back_inserter(protocol_), ptr_fun<int,int>(tolower));
  if (protocol_iter == end_iter) {
    return;
  }
    
  advance(protocol_iter, protocol_end.length());

  string::const_iterator path_iter = find(protocol_iter, end_iter, '/');
  string::const_iterator port_iter = find(protocol_iter, path_iter, ':');

  // host is case insensitive
  host_.reserve(distance(protocol_iter, port_iter));
  transform(protocol_iter, port_iter, back_inserter(host_), ptr_fun<int,int>(tolower));

  if (port_iter != path_iter) {
    ++port_iter;
    port_.assign(port_iter, path_iter);
    
    // Convert port into int16_t
    const char *start_ptr = port_.c_str();
    char *end_ptr;
    errno = 0;
    long port_val = strtol(start_ptr, &end_ptr, 10);
    if (errno == ERANGE || port_val > UINT16_MAX){
      throw system_error(ERANGE, std::generic_category(), "URL has a bad port #");
    }
    if (start_ptr != end_ptr) {
      nport_ = (uint16_t)port_val;
    }
  }

  string::const_iterator query_iter = find(path_iter, end_iter, '?');
  path_.assign(path_iter, query_iter);

  if (query_iter != end_iter) {
    ++query_iter;
    query_.assign(query_iter, end_iter);
  }
}

