#pragma once
#include "DataTable.hpp"
#include <boost/describe.hpp>
#include <boost/mp11.hpp>
#include <type_traits>
#include <vector>
#include <string>
#include <cctype>

namespace omnisphere::data {

template<typename T> struct is_optional : std::false_type {};
template<typename T> struct is_optional<std::optional<T>> : std::true_type {};

template<typename T> struct extract_optional { using type = T; };
template<typename T> struct extract_optional<std::optional<T>> { using type = T; };

template<class T>
void MapMembersRecursive(T& item, omnisphere::types::DataTable::Row& row, const std::string& prefix) {
    // Map own members
    boost::mp11::mp_for_each< boost::describe::describe_members<T, boost::describe::mod_public> >(
        [&](auto PropertyDescriptor) {
            std::string rawName = PropertyDescriptor.name;
            std::string columnName = prefix + rawName;
            std::string pascalName = prefix + rawName;
            if (!pascalName.empty() && std::islower(static_cast<unsigned char>(pascalName[0]))) {
                pascalName[0] = static_cast<char>(std::toupper(static_cast<unsigned char>(pascalName[0])));
            }

            std::string targetCol;
            if (row.HasColumn(columnName)) {
                targetCol = columnName;
            } else if (row.HasColumn(pascalName)) {
                targetCol = pascalName;
            }

            if (!targetCol.empty()) {
                using FieldType = std::remove_reference_t<decltype(item.*PropertyDescriptor.pointer)>;
                if constexpr (is_optional<FieldType>::value) {
                    using Underlying = typename extract_optional<FieldType>::type;
                    item.*PropertyDescriptor.pointer = row[targetCol].template GetOptional<Underlying>();
                } else {
                    item.*PropertyDescriptor.pointer = static_cast<FieldType>(row[targetCol]);
                }
            }
        }
    );

    // Map base classes members recursively
    boost::mp11::mp_for_each< boost::describe::describe_bases<T, boost::describe::mod_any_access> >(
        [&](auto BaseDescriptor) {
            using BaseType = typename decltype(BaseDescriptor)::type;
            MapMembersRecursive<BaseType>(item, row, prefix); // implicit upcast of item
        }
    );
}

template<class T>
T MapFromRow(const omnisphere::types::DataTable::Row& row, const std::string& prefix = "") {
    T item{};
    MapMembersRecursive<T>(item, const_cast<omnisphere::types::DataTable::Row&>(row), prefix);
    return item;
}

template<class T>
std::vector<T> MapFromDataTable(const omnisphere::types::DataTable& dt, const std::string& prefix = "") {
    std::vector<T> list;
    list.reserve(dt.RowsCount());
    for (size_t i = 0; i < dt.RowsCount(); ++i) {
        list.push_back(MapFromRow<T>(dt[i], prefix));
    }
    return list;
}

} // namespace omnisphere::data

namespace omnisphere::types {

template<class T>
inline T FromDataRow(const omnisphere::types::DataTable::Row& row, const std::string& prefix = "") {
    return omnisphere::data::MapFromRow<T>(row, prefix);
}

template<class T>
inline std::vector<T> DataTableToModels(const omnisphere::types::DataTable& dt, const std::string& prefix = "") {
    return omnisphere::data::MapFromDataTable<T>(dt, prefix);
}

} // namespace omnisphere::types
