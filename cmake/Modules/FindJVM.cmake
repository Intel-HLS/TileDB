# Setup jvm variables for include/link

set(JAVA_HOME "$ENV{JAVA_HOME}")
if(NOT JAVA_HOME)
  message(FATAL_ERROR "Undefined JAVA_HOME environment variable")
  return()
endif()
if(NOT EXISTS ${JAVA_HOME})
  message(FATAL_ERROR "JAVA_HOME=" ${JAVA_HOME} " NOT found")
  return()
endif()
if(NOT IS_DIRECTORY ${JAVA_HOME})
  message(FATAL_ERROR "JAVA_HOME=" ${JAVA_HOME} " NOT a directory")
  return()
endif()

message(STATUS "JAVA_HOME=" ${JAVA_HOME})

if (NOT IS_DIRECTORY ${JAVA_HOME}/include)
  message(FATAL_ERROR "JAVA_HOME=" ${JAVA_HOME}
	" does NOT seem to contain an include folder")
  return()
set(JVM_INCLUDE_PATH ${JAVA_HOME}/include)

find_library(JVM_LIBRARY jvm PATHS ${JAVA_HOME}/jre/lib/amd64/server)
if(JVM_LIBRARY)
  message(STATUS "Found jvm library in " ${JVM_LIBRARY})
else()
  message(FATAL_ERROR "jvm library could not be found")
  return()
endif()
