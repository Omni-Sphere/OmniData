cmake_minimum_required(VERSION 3.16)
project(OmniData LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_WINDOWS_EXPORT_ALL_SYMBOLS ON)

if(WIN32)
    set(DEST_DIR ${CMAKE_BINARY_DIR}/OmniData)
elseif(APPLE)
    set(DEST_DIR ${CMAKE_BINARY_DIR}/macOS)
else()
    set(DEST_DIR ${CMAKE_BINARY_DIR}/OmniData)
endif()

set(DEST_INCLUDE_DIR "${DEST_DIR}/Include")
set(DEST_LIB_DIR "${DEST_DIR}/Libs")

add_library(OmniData SHARED
    DataTable.cpp
    Database.cpp
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
    RUNTIME_OUTPUT_DIRECTORY ${DEST_LIB_DIR}
    LIBRARY_OUTPUT_DIRECTORY ${DEST_LIB_DIR}
    ARCHIVE_OUTPUT_DIRECTORY ${DEST_DIR}
)

if(WIN32)
    get_filename_component(SODIUM_LIB_DIR "${SODIUM_LIB}" DIRECTORY)

    find_file(SODIUM_DLL 
        NAMES "libsodium.dll" "sodium.dll" "libsodium-26.dll"
        HINTS "${SODIUM_LIB_DIR}/../bin" "${SODIUM_LIB_DIR}"
    )
    
    if(SODIUM_DLL)
        message(STATUS "Found Sodium DLL: ${SODIUM_DLL}")
        add_custom_command(TARGET OmniData POST_BUILD
            COMMAND ${CMAKE_COMMAND} -E copy_if_different "${SODIUM_DLL}" "${DEST_LIB_DIR}"
            COMMENT "Copying Sodium DLL..."
        )
    else()
        message(WARNING "Could NOT find Sodium DLL. You may need to copy it manually to ${DEST_LIB_DIR}")
    endif()

    set(BOOST_BIN_DIR "")
    if(DEFINED Boost_LIBRARY_DIRS)
        list(GET Boost_LIBRARY_DIRS 0 FIRST_LIB_DIR)
        set(BOOST_BIN_DIR "${FIRST_LIB_DIR}/../bin")
    elseif(DEFINED Boost_LIBRARIES)
        list(GET Boost_LIBRARIES 0 FIRST_LIB)
        if(EXISTS "${FIRST_LIB}")
            get_filename_component(FIRST_LIB_DIR "${FIRST_LIB}" DIRECTORY)
            set(BOOST_BIN_DIR "${FIRST_LIB_DIR}/../bin")
        endif()
    endif()

    if(BOOST_BIN_DIR AND EXISTS "${BOOST_BIN_DIR}")
        file(GLOB BOOST_DLLS "${BOOST_BIN_DIR}/*boost_json*.dll" "${BOOST_BIN_DIR}/*boost_container*.dll")
        foreach(DLL ${BOOST_DLLS})
            add_custom_command(TARGET OmniData POST_BUILD
                COMMAND ${CMAKE_COMMAND} -E copy_if_different "${DLL}" "${DEST_LIB_DIR}"
                COMMENT "Copying Boost DLL: ${DLL}"
            )
        endforeach()
    endif()
endif()

set(PUBLIC_HEADERS
    ${CMAKE_CURRENT_SOURCE_DIR}/Database.hpp
    ${CMAKE_CURRENT_SOURCE_DIR}/DataTable.hpp
    ${CMAKE_CURRENT_SOURCE_DIR}/SQLParams.hpp
)

foreach(HDR ${PUBLIC_HEADERS})
    file(RELATIVE_PATH REL_PATH "${CMAKE_CURRENT_SOURCE_DIR}" "${HDR}")
    set(DEST_PATH "${DEST_INCLUDE_DIR}/${REL_PATH}")

    get_filename_component(DEST_INC_DIR "${DEST_PATH}" DIRECTORY)
    add_custom_command(TARGET OmniData POST_BUILD
        COMMAND ${CMAKE_COMMAND} -E make_directory "${DEST_INC_DIR}"
        COMMAND ${CMAKE_COMMAND} -E copy "${HDR}" "${DEST_PATH}"
    )
endforeach()

# Installation rules for Linux
if(UNIX AND NOT APPLE)
    install(TARGETS OmniData
        LIBRARY DESTINATION /usr/local/lib/OmniSphere
        ARCHIVE DESTINATION /usr/local/lib/OmniSphere
    )
    
    foreach(HDR ${PUBLIC_HEADERS})
        file(RELATIVE_PATH REL_PATH "${CMAKE_CURRENT_SOURCE_DIR}" "${HDR}")
        get_filename_component(HDR_DIR "${REL_PATH}" DIRECTORY)
        install(FILES "${HDR}"
            DESTINATION "/usr/local/include/OmniSphere/OmniData/${HDR_DIR}"
        )
    endforeach()
    
    install(CODE "
        file(WRITE \"/etc/ld.so.conf.d/omnisphere-omnidata.conf\" \"/usr/local/lib/OmniSphere\\n\")
        execute_process(COMMAND ldconfig)
        message(STATUS \"Configured dynamic linker for OmniSphere libraries\")
    ")
endif()
