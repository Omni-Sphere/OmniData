#pragma once

#include <variant>
#include <string>
#include <vector>
#include <optional>

namespace omnicore::type
{

    using SQLParam = std::variant<
        std::monostate,
        bool,
        int,
        std::string,
        std::vector<uint8_t>,
        double>;

    template <typename T>
    inline SQLParam MakeSQLParam(const std::optional<T> &opt)
    {
        if (opt.has_value())
            return SQLParam{*opt};
        else
            return SQLParam{std::monostate{}};
    }

    inline SQLParam MakeSQLParam(int val)
    {
        return SQLParam{val};
    }

    inline SQLParam MakeSQLParam(const std::string &val)
    {
        return SQLParam{val};
    }

    inline SQLParam MakeSQLParam(const std::vector<uint8_t> &val)
    {
        return SQLParam{val};
    }

    inline SQLParam MakeSQLParam(bool val)
    {
        return SQLParam{val};
    }

    inline SQLParam MakeSQLParam(double val)
    {
        return SQLParam{val};
    }

    template <typename T, typename = std::enable_if_t<std::is_enum_v<T>>>
    inline SQLParam MakeSQLParam(T val)
    {
        return SQLParam{static_cast<std::underlying_type_t<T>>(val)};
    }
}