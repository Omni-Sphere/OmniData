cmake_minimum_required(VERSION 3.16)
project(OmniData LANGUAGES CXX)

# --- 0. Detección de la Raíz de Proyectos (Sin usar "../") ---
get_filename_component(PROYECTOS_ROOT "${CMAKE_CURRENT_SOURCE_DIR}" DIRECTORY)

# --- Configuración inteligente para compilación manual ---
if(CMAKE_INSTALL_PREFIX_INITIALIZED_TO_DEFAULT)
    if(CMAKE_BUILD_TYPE STREQUAL "Release")
        set(CMAKE_INSTALL_PREFIX "/usr/local" CACHE PATH "Directorio de instalación" FORCE)
    else()
        set(CMAKE_INSTALL_PREFIX "${PROYECTOS_ROOT}/.omni-dev-sdk" CACHE PATH "Directorio de instalación" FORCE)
    endif()
endif()

include(GNUInstallDirs)

# --- 1. Resolución de Rutas de las OmniLibs ---
if(CMAKE_BUILD_TYPE STREQUAL "Release")
    set(OMNI_BASE_DIR "/usr/local")
else()
    set(OMNI_BASE_DIR "${PROYECTOS_ROOT}/.omni-dev-sdk")
endif()

if(EXISTS "${OMNI_BASE_DIR}")
    list(APPEND CMAKE_PREFIX_PATH "${OMNI_BASE_DIR}")
    # Aseguramos que encuentre los Config.cmake de OmniUtils en su nueva ruta
    list(APPEND CMAKE_PREFIX_PATH "${OMNI_BASE_DIR}/lib/OmniUtils/cmake/OmniUtils")
endif()

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
    PostgreSQLDatabase.cpp
    DatabasePool.cpp
)

add_library(OmniData::OmniData ALIAS OmniData)

target_include_directories(OmniData
    PUBLIC
        $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}>
        $<INSTALL_INTERFACE:${CMAKE_INSTALL_INCLUDEDIR}>
)

# --- Resolución de dependencia OmniUtils ---
if(NOT TARGET OmniUtils::OmniUtils)
    find_package(OmniUtils REQUIRED)
endif()

# --- Corrección limpia de rutas para OmniUtils importado ---
if(TARGET OmniUtils::OmniUtils)
    get_target_property(IS_IMPORTED OmniUtils::OmniUtils IMPORTED)
    if(IS_IMPORTED)
        set_target_properties(OmniUtils::OmniUtils PROPERTIES
            INTERFACE_INCLUDE_DIRECTORIES "${OMNI_BASE_DIR}/include"
        )
    endif()
endif()

# --- Dependencias por plataforma ---
# ODBC se mantiene SOLO para SQLServerDatabase.
# PostgreSQL usa libpqxx (C++ wrapper — incluye libpq transitivamente).
# MySQL usa Connector/C (libmysqlclient / mariadb-libs).

if(ANDROID)
    # Android: solo SQLServer vía ODBC stub; PG y MySQL excluidos
    target_link_libraries(OmniData PUBLIC
        OmniUtils::OmniUtils
        Boost::json
    )

elseif(WIN32)
    find_library(SODIUM_LIB sodium REQUIRED)
    find_package(Boost REQUIRED COMPONENTS json)

    # PostgreSQL: libpqxx C++ wrapper (incluye libpq transitivamente)
    # Windows: instalar via vcpkg: vcpkg install libpqxx
    find_package(libpqxx REQUIRED)

    # MySQL Connector/C (via vcpkg: mysql-connector-c)
    # On Arch Linux: install mariadb-libs (provides mysqlclient-compatible headers)
    find_library(MYSQLCLIENT_LIB
        NAMES mysqlclient libmysqlclient mariadb
        REQUIRED
    )

    target_link_libraries(OmniData PUBLIC
        odbc32                    # SQL Server solamente
        libpqxx::pqxx             # PostgreSQL (libpqxx → libpq)
        ${MYSQLCLIENT_LIB}        # MySQL Connector/C
        ${SODIUM_LIB}
        Boost::json
        OmniUtils::OmniUtils
    )

elseif(APPLE)
    find_library(ODBC_LIB odbc REQUIRED)
    find_library(SODIUM_LIB sodium REQUIRED)
    find_package(Boost REQUIRED COMPONENTS json)
    # macOS: brew install libpqxx
    find_package(libpqxx REQUIRED)
    find_library(MYSQLCLIENT_LIB
        NAMES mysqlclient libmysqlclient mariadb
        HINTS /usr/local/lib /opt/homebrew/lib
        REQUIRED
    )

    target_link_libraries(OmniData PUBLIC
        ${ODBC_LIB}               # SQL Server
        libpqxx::pqxx             # PostgreSQL
        ${MYSQLCLIENT_LIB}
        ${SODIUM_LIB}
        Boost::json
        OmniUtils::OmniUtils
    )

else() # Linux
    find_package(ODBC REQUIRED)
    find_library(SODIUM_LIB sodium REQUIRED)
    find_package(Boost REQUIRED COMPONENTS json)

    find_package(libpqxx QUIET)
    if(NOT TARGET libpqxx::pqxx)
        find_package(PkgConfig REQUIRED)
        pkg_check_modules(libpqxx REQUIRED IMPORTED_TARGET libpqxx)
        add_library(libpqxx::pqxx ALIAS PkgConfig::libpqxx)
    endif()

    find_library(MYSQLCLIENT_LIB
        NAMES mysqlclient libmysqlclient mariadb
        HINTS /usr/lib/x86_64-linux-gnu /usr/lib /usr/local/lib /usr/lib64
        REQUIRED
    )

    target_link_libraries(OmniData PUBLIC
        ODBC::ODBC                # SQL Server
        libpqxx::pqxx             # PostgreSQL (libpqxx → libpq transitivo)
        ${MYSQLCLIENT_LIB}
        ${SODIUM_LIB}
        Boost::json
        OmniUtils::OmniUtils
    )
endif()

set_target_properties(OmniData PROPERTIES
    RUNTIME_OUTPUT_DIRECTORY "${LIBS_DIR}"
    LIBRARY_OUTPUT_DIRECTORY "${LIBS_DIR}"
    ARCHIVE_OUTPUT_DIRECTORY "${ARCHIVE_DIR}"
)

if(WIN32)
    # Eliminamos el uso de "../bin" extrayendo el directorio padre directamente
    get_filename_component(SODIUM_LIB_DIR "${SODIUM_LIB}" DIRECTORY)
    get_filename_component(SODIUM_PARENT_DIR "${SODIUM_LIB_DIR}" DIRECTORY)
    find_file(SODIUM_DLL 
        NAMES "libsodium.dll" "sodium.dll" "libsodium-26.dll"
        HINTS "${SODIUM_PARENT_DIR}/bin" "${SODIUM_LIB_DIR}"
    )
    if(SODIUM_DLL)
        add_custom_command(TARGET OmniData POST_BUILD
            COMMAND ${CMAKE_COMMAND} -E copy_if_different "${SODIUM_DLL}" "${LIBS_DIR}"
        )
    endif()

    if(DEFINED Boost_LIBRARY_DIRS)
        list(GET Boost_LIBRARY_DIRS 0 FIRST_LIB_DIR)
        # Eliminamos el uso de "../bin" en Boost también
        get_filename_component(BOOST_PARENT_DIR "${FIRST_LIB_DIR}" DIRECTORY)
        set(BOOST_BIN_DIR "${BOOST_PARENT_DIR}/bin")
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
    ${CMAKE_CURRENT_SOURCE_DIR}/PostgreSQLDatabase.hpp
    ${CMAKE_CURRENT_SOURCE_DIR}/DatabasePool.hpp
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

# --- Installation & Export ---
install(TARGETS OmniData
    EXPORT OmniDataTargets
    RUNTIME DESTINATION ${CMAKE_INSTALL_BINDIR}
    LIBRARY DESTINATION ${CMAKE_INSTALL_LIBDIR}
    ARCHIVE DESTINATION ${CMAKE_INSTALL_LIBDIR}
    INCLUDES DESTINATION ${CMAKE_INSTALL_INCLUDEDIR}
)

install(DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}/
    DESTINATION ${CMAKE_INSTALL_INCLUDEDIR}/${PROJECT_NAME}
    FILES_MATCHING PATTERN "*.hpp" PATTERN "*.h"
    PATTERN "build" EXCLUDE
    PATTERN ".git" EXCLUDE
    PATTERN ".vscode" EXCLUDE
)

file(WRITE "${CMAKE_CURRENT_BINARY_DIR}/OmniDataConfig.cmake"
"include(CMakeFindDependencyMacro)\n"
"find_dependency(Boost REQUIRED COMPONENTS json)\n"
"find_dependency(OmniUtils REQUIRED)\n"
"if(NOT ANDROID)\n"
"  # ODBC: requerido para SQL Server en todas las plataformas no-Android\n"
"  if(WIN32)\n"
"    find_dependency(ODBC REQUIRED)\n"
"  elseif(NOT ANDROID)\n"
"    find_dependency(ODBC REQUIRED)\n"
"  endif()\n"
"  # PostgreSQL nativo (libpqxx)\n"
"  find_dependency(libpqxx REQUIRED)\n"
"endif()\n"
"include(\"\${CMAKE_CURRENT_LIST_DIR}/OmniDataTargets.cmake\")\n"
)

install(FILES "${CMAKE_CURRENT_BINARY_DIR}/OmniDataConfig.cmake"
    DESTINATION ${CMAKE_INSTALL_LIBDIR}/cmake/OmniData
)

install(EXPORT OmniDataTargets
    FILE OmniDataTargets.cmake
    NAMESPACE OmniData::
    DESTINATION ${CMAKE_INSTALL_LIBDIR}/cmake/OmniData
)