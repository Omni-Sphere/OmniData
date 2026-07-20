#pragma once
#include "DataTable.hpp"
#include <boost/describe.hpp>
#include <boost/mp11.hpp>

#include <type_traits>

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
            std::string columnName = prefix + PropertyDescriptor.name;
            if (row.HasColumn(columnName)) {
                using FieldType = std::remove_reference_t<decltype(item.*PropertyDescriptor.pointer)>;
                if constexpr (is_optional<FieldType>::value) {
                    using Underlying = typename extract_optional<FieldType>::type;
                    item.*PropertyDescriptor.pointer = row[columnName].template GetOptional<Underlying>();
                } else {
                    item.*PropertyDescriptor.pointer = static_cast<FieldType>(row[columnName]);
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
T MapFromRow(omnisphere::types::DataTable::Row& row, const std::string& prefix = "") {
    T item;
    MapMembersRecursive<T>(item, row, prefix);
    return item;
}

} // namespace omnisphere::data
