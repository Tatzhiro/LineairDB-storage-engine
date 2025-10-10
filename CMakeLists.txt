# Copyright (c) 2006, 2021, Oracle and/or its affiliates.
#
# This program is free software; you can redistribute it and/or modify it under
# the terms of the GNU General Public License, version 2.0, as published by the
# Free Software Foundation.
#
# This program is also distributed with certain software (including but not
# limited to OpenSSL) that is licensed under separate terms, as designated in a
# particular file or component or in included license documentation.  The
# authors of MySQL hereby grant you an additional permission to link the program
# and your derivative works with the separately licensed software that they have
# included with MySQL.
#
# This program is distributed in the hope that it will be useful, but WITHOUT
# ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS
# FOR A PARTICULAR PURPOSE.  See the GNU General Public License, version 2.0,
# for more details.
#
# You should have received a copy of the GNU General Public License along with
# this program; if not, write to the Free Software Foundation, Inc., 51 Franklin
# St, Fifth Floor, Boston, MA 02110-1301  USA

set(CMAKE_CXX_STANDARD 17)

add_subdirectory(third_party/LineairDB)
set(LINEAIRDB_PLUGIN_DYNAMIC "ha_lineairdb")
set(LINEAIRDB_SOURCES ha_lineairdb.cc ha_lineairdb.hh 
                      lineairdb_field.cc lineairdb_field.hh
                      lineairdb_transaction.cc lineairdb_transaction.hh)
add_definitions(-DMYSQL_SERVER)
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -Wno-error -Wextra -mcx16 -fPIC")

if(WITH_LINEAIRDB_STORAGE_ENGINE AND NOT WITHOUT_LINEAIRDB_STORAGE_ENGINE)
  mysql_add_plugin(lineairdb_storage_engine ${LINEAIRDB_SOURCES} STORAGE_ENGINE
                   DEFAULT LINK_LIBRARIES linearidb::lineairdb)
elseif(NOT WITHOUT_LINEAIRDB_STORAGE_ENGINE)
  mysql_add_plugin(lineairdb_storage_engine ${LINEAIRDB_SOURCES} STORAGE_ENGINE
                   MODULE_ONLY LINK_LIBRARIES lineairdb::lineairdb)
endif()

# Ensure InnoDB headers and mutex type definitions are visible to this plugin
if (TARGET lineairdb_storage_engine)
  target_include_directories(lineairdb_storage_engine PRIVATE
    ${CMAKE_SOURCE_DIR}/storage/innobase
    ${CMAKE_SOURCE_DIR}/storage/innobase/include
    ${CMAKE_SOURCE_DIR}/storage/innobase/ut
  )

  if (MUTEXTYPE MATCHES "event")
    target_compile_definitions(lineairdb_storage_engine PRIVATE MUTEX_EVENT)
  elseif (MUTEXTYPE MATCHES "futex")
    target_compile_definitions(lineairdb_storage_engine PRIVATE MUTEX_FUTEX)
  else()
    target_compile_definitions(lineairdb_storage_engine PRIVATE MUTEX_SYS)
  endif()

  # Avoid static TLS allocation failure on dynamic load
  target_compile_options(lineairdb_storage_engine PRIVATE -ftls-model=global-dynamic)
endif()
