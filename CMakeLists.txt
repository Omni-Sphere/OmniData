cmake_minimum_required(VERSION 3.16)
project(OmniData LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_WINDOWS_EXPORT_ALL_SYMBOLS ON)

set(INCLUDE_DIR "${CMAKE_BINARY_DIR}/OmniData/Include")
set(LIBS_DIR "${CMAKE_BINARY_DIR}/OmniData")

if(WIN32)
    set(ARCHIVE_DIR "${CMAKE_BINARY_DIR}/OmniData/Libs")
else()
    set(ARCHIVE_DIR "${LIBS_DIR}")
endif()

add_library(OmniData SHARED
    DataTable.cpp
    Database.cpp
    SQLServerDatabase.cpp
    MySQLDatabase.cpp
)

target_include_directories(OmniData
    PUBLIC
        ${CMAKE_CURRENT_SOURCE_DIR}
)

if(WIN32)
    target_link_libraries(OmniData PUBLIC odbc32)
elseif(APPLE)
    find_library(ODBC_LIB odbc REQUIRED)
    target_link_libraries(OmniData PUBLIC ${ODBC_LIB})
else()
    find_package(ODBC REQUIRED)
    target_link_libraries(OmniData PUBLIC ODBC::ODBC)
endif()

find_library(SODIUM_LIB sodium REQUIRED)
find_package(Boost REQUIRED COMPONENTS json)

target_link_libraries(OmniData PUBLIC
    ${SODIUM_LIB}
    Boost::json
)

set_target_properties(OmniData PROPERTIES
    RUNTIME_OUTPUT_DIRECTORY "${LIBS_DIR}"
    LIBRARY_OUTPUT_DIRECTORY "${LIBS_DIR}"
    ARCHIVE_OUTPUT_DIRECTORY "${ARCHIVE_DIR}"
)

if(WIN32)
    get_filename_component(SODIUM_LIB_DIR "${SODIUM_LIB}" DIRECTORY)
    find_file(SODIUM_DLL 
        NAMES "libsodium.dll" "sodium.dll" "libsodium-26.dll"
        HINTS "${SODIUM_LIB_DIR}/../bin" "${SODIUM_LIB_DIR}"
    )
    if(SODIUM_DLL)
        add_custom_command(TARGET OmniData POST_BUILD
            COMMAND ${CMAKE_COMMAND} -E copy_if_different "${SODIUM_DLL}" "${LIBS_DIR}"
        )
    endif()

    if(DEFINED Boost_LIBRARY_DIRS)
        list(GET Boost_LIBRARY_DIRS 0 FIRST_LIB_DIR)
        set(BOOST_BIN_DIR "${FIRST_LIB_DIR}/../bin")
        file(GLOB BOOST_DLLS "${BOOST_BIN_DIR}/*boost_json*.dll")
        foreach(DLL ${BOOST_DLLS})
            add_custom_command(TARGET OmniData POST_BUILD
                COMMAND ${CMAKE_COMMAND} -E copy_if_different "${DLL}" "${LIBS_DIR}"
            )
        endforeach()
    endif()
endif()

set(PUBLIC_HEADERS
    ${CMAKE_CURRENT_SOURCE_DIR}/Database.hpp
    ${CMAKE_CURRENT_SOURCE_DIR}/IDatabase.hpp
    ${CMAKE_CURRENT_SOURCE_DIR}/SQLServerDatabase.hpp
    ${CMAKE_CURRENT_SOURCE_DIR}/MySQLDatabase.hpp
    ${CMAKE_CURRENT_SOURCE_DIR}/DataTable.hpp
    ${CMAKE_CURRENT_SOURCE_DIR}/SQLParams.hpp
)

foreach(HDR ${PUBLIC_HEADERS})
    file(RELATIVE_PATH REL_PATH "${CMAKE_CURRENT_SOURCE_DIR}" "${HDR}")
    
    set(DEST_PATH "${INCLUDE_DIR}/${REL_PATH}")

    get_filename_component(DEST_INC_DIR "${DEST_PATH}" DIRECTORY)
    
    add_custom_command(TARGET OmniData POST_BUILD
        COMMAND ${CMAKE_COMMAND} -E make_directory "${DEST_INC_DIR}"
        COMMAND ${CMAKE_COMMAND} -E copy_if_different "${HDR}" "${DEST_PATH}"
        COMMENT "Copiando header: ${REL_PATH}"
    )
endforeach()
